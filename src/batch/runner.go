package batch

import (
	"context"
	"errors"
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/src/data"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/knadh/koanf"
	"time"
)

type Runner struct {
	Version string
}

func (runner *Runner) Run(ctx context.Context, config *koanf.Koanf) error {
	begin := time.Now()
	if err := database.Connect(config.String("dsn")); err != nil {
		return err
	}

	if config.Bool("new") {
		fmt.Println("execute DDL update...")
		if err := RunDDL(database.DB); err != nil {
			return err
		}
	}

	dataRepo := data.NewDataRepository(database.DB)

	if config.Bool("update") {
		fmt.Println("begin update...")
		if updated, err := data.UpdateData(ctx, dataRepo); err != nil {
			return err
		} else {
			fmt.Printf("update affected: %+v rows\n", updated)
		}
	}

	plan, err := data.FetchImportPlan(config, dataRepo)
	if err != nil {
		return err
	}

	sqlDB, err := database.DB.DB()
	if err != nil {
		return fmt.Errorf("open SQL connection pool: %w", err)
	}
	coordinator := NewImportExecutionCoordinator(sqlDB, runner.Version)
	preparation, err := coordinator.Prepare(
		ctx,
		plan.Dumps,
		config.Bool("force"),
		config.Bool("allow-downgrade"),
	)
	if err != nil {
		return err
	}
	if preparation.Skipped {
		fmt.Printf(
			"manifest %s already succeeded as import run %d; skipping\n",
			preparation.ManifestSHA256,
			preparation.RunID,
		)
		return nil
	}

	var (
		b            = New()
		totalUpdates = 0
		chunk        = config.Int("chunk")
		db           = database.DB
		steps        = make([]Step, 0)
	)

	if hasArtist(config) {
		order := NewOrder(ctx, chunk, plan.Resources["artists"], db)
		steps = append(steps, b.UpdateArtist(order))
	}

	if hasLabel(config) {
		order := NewOrder(ctx, chunk, plan.Resources["labels"], db)
		steps = append(steps, b.UpdateLabel(order))
	}

	if hasMaster(config) {
		order := NewOrder(ctx, chunk, plan.Resources["masters"], db)
		steps = append(steps, b.UpdateMaster(order))
	}

	if hasRelease(config) {
		order := NewOrder(ctx, chunk, plan.Resources["releases"], db)
		steps = append(steps, b.UpdateRelease(order))
	}

	for i := range steps {
		r := steps[i]()
		totalUpdates += r.Count()
		if r.IsErr() {
			err = r.Err()
			break
		}
	}

	completionErr := coordinator.Complete(ctx, err)
	if completionErr != nil {
		err = errors.Join(err, completionErr)
	}
	printResult(begin, totalUpdates, err)
	return err
}

func printResult(begin time.Time, total int, err error) {
	took := time.Since(begin).Truncate(time.Second).String()
	s := fmt.Sprintf("updated %+v records in %+v.", total, took)
	if err != nil {
		s += fmt.Sprintf(" [error: %+v]", err)
	}
	fmt.Println(s)
}
