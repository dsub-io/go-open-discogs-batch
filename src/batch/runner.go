package batch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/data"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	fileutil "github.com/dsub-io/go-open-discogs-batch/src/file"
	"github.com/knadh/koanf"
	"gorm.io/gorm"
)

type Runner struct {
	Version string
}

func (runner *Runner) Run(ctx context.Context, config *koanf.Koanf) error {
	begin := time.Now()
	if err := database.Connect(config.String("database-url")); err != nil {
		return err
	}

	fmt.Println("execute DDL update...")
	if err := RunDDL(database.DB); err != nil {
		return err
	}

	dataRepo := data.NewDataRepository(database.DB)
	fmt.Println("refreshing dump catalog...")
	updated, updateErr := data.UpdateSelectedData(
		ctx,
		dataRepo,
		config.Strings("entities"),
		config.String("dump-month"),
	)
	if updateErr != nil {
		fmt.Printf("dump catalog refresh failed; trying cached catalog: %v\n", updateErr)
	} else {
		fmt.Printf("dump catalog refresh affected: %+v rows\n", updated)
	}

	plan, err := data.FetchImportPlan(config, dataRepo)
	if err != nil {
		return errors.Join(updateErr, err)
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
		if config.Bool("cleanup") {
			return cleanupImportFiles(plan)
		}
		return nil
	}

	var (
		b            = New()
		totalUpdates = 0
		chunk        = config.Int("chunk-size")
		maxWorkers   = config.Int("max-workers")
	)
	fmt.Printf("max-workers=%d\n", maxWorkers)
	steps := buildImportSteps(ctx, config, plan, b, chunk, maxWorkers, database.DB)

	for i := range steps {
		r := steps[i]()
		totalUpdates += r.Count()
		if r.IsErr() {
			err = r.Err()
			break
		}
	}

	if err == nil && config.Bool("cleanup") {
		err = cleanupImportFiles(plan)
	}
	completionErr := coordinator.Complete(ctx, err)
	if completionErr != nil {
		err = errors.Join(err, completionErr)
	}
	printResult(begin, totalUpdates, err)
	return err
}

func buildImportSteps(
	ctx context.Context,
	config *koanf.Koanf,
	plan *data.ImportPlan,
	batch Batch,
	chunkSize int,
	maxWorkers int,
	db *gorm.DB,
) []Step {
	definitions := []struct {
		enabled     bool
		resourceKey string
		build       func(Order) Step
	}{
		{hasArtist(config), "artists", batch.UpdateArtist},
		{hasLabel(config), "labels", batch.UpdateLabel},
		{hasMaster(config), "masters", batch.UpdateMaster},
		{hasRelease(config), "releases", batch.UpdateRelease},
	}
	steps := make([]Step, 0, len(definitions))
	for _, definition := range definitions {
		if !definition.enabled {
			continue
		}
		order := NewOrder(
			ctx,
			chunkSize,
			maxWorkers,
			plan.Resources[definition.resourceKey],
			db,
		)
		steps = append(steps, definition.build(order))
	}
	return steps
}

func cleanupImportFiles(plan *data.ImportPlan) error {
	paths := make([]string, 0, len(plan.Resources))
	seen := make(map[string]bool)
	for _, resourcePath := range plan.Resources {
		if !seen[resourcePath] {
			seen[resourcePath] = true
			paths = append(paths, resourcePath)
		}
	}
	sort.Strings(paths)
	handler := fileutil.NewHandler()
	var cleanupErr error
	for _, resourcePath := range paths {
		if err := handler.Delete(resourcePath); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete %s: %w", resourcePath, err))
		}
	}
	return cleanupErr
}

func printResult(begin time.Time, total int, err error) {
	took := time.Since(begin).Truncate(time.Second).String()
	s := fmt.Sprintf("updated %+v records in %+v.", total, took)
	if err != nil {
		s += fmt.Sprintf(" [error: %+v]", err)
	}
	fmt.Println(s)
}
