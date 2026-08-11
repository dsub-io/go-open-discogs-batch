package batch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/data"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	fileutil "github.com/dsub-io/go-open-discogs-batch/src/file"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"gorm.io/gorm"
)

type Runner struct {
	Version string
}

const importCompletionTimeout = 30 * time.Second

const publicSchemaWarning = "WARN: database schema is public; set --database-schema or OPEN_DISCOGS_BATCH_DATABASE_SCHEMA to isolate OpenDiscogs tables"

type importCompleter interface {
	Complete(context.Context, error) error
}

type importExecutionCoordinator interface {
	importCompleter
	Prepare(context.Context, []*opendiscogsmodel.DiscogsDump, int, bool, bool) (*ImportPreparation, error)
}

var connectDatabase = database.ConnectInSchema
var configureDatabasePool = database.ConfigurePool
var ensureDatabaseSchema = database.EnsureSchema
var runDatabaseDDL = RunDDLInSchema
var refreshSelectedData = data.UpdateSelectedData
var resolveImportPlan = data.ResolveImportPlan
var fetchImportResources = data.FetchImportResources
var openSQLDatabase = func(db *gorm.DB) (*sql.DB, error) { return db.DB() }
var newExecutionCoordinator = func(db *sql.DB, version string) importExecutionCoordinator {
	return NewImportExecutionCoordinator(db, version)
}
var preloadImportReferenceIDs = preloadReferenceIDs
var newImportBatch = New
var closeIdentifierRows = func(rows *sql.Rows) error { return rows.Close() }

func (runner *Runner) Run(ctx context.Context, config *koanf.Koanf) error {
	var (
		begin      = time.Now()
		chunk      = config.Int("chunk-size")
		maxWorkers = config.Int("max-workers")
		schemaName = config.String("database-schema")
	)
	schema, err := database.ParseSchema(schemaName)
	if err != nil {
		return err
	}
	if schema.Name() == database.DefaultSchemaName {
		fmt.Fprintln(os.Stderr, publicSchemaWarning)
	}
	if err := connectDatabase(config.String("database-url"), schemaName); err != nil {
		return err
	}
	if err := configureDatabasePool(database.DB, maxWorkers); err != nil {
		return err
	}
	created, err := ensureDatabaseSchema(database.DB, schema)
	if err != nil {
		return err
	}
	fmt.Printf("database schema ready: %s (created=%t)\n", schema.Name(), created)

	fmt.Println("execute DDL update...")
	if err := runDatabaseDDL(database.DB, schemaName); err != nil {
		return err
	}

	dataRepo := data.NewDataRepository(database.DB)
	fmt.Println("refreshing dump catalog...")
	updated, updateErr := refreshSelectedData(
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

	plan, err := resolveImportPlan(config, dataRepo)
	if err != nil {
		return errors.Join(updateErr, err)
	}

	sqlDB, err := openSQLDatabase(database.DB)
	if err != nil {
		return fmt.Errorf("open SQL connection pool: %w", err)
	}
	coordinator := newExecutionCoordinator(sqlDB, runner.Version)
	preparation, err := coordinator.Prepare(
		ctx,
		plan.Dumps,
		chunk,
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
	if err := fetchImportResources(ctx, plan); err != nil {
		return finalizeImport(ctx, coordinator, plan, false, err)
	}
	cache.ResetIDs()
	if err := preloadImportReferenceIDs(ctx, sqlDB, config); err != nil {
		return finalizeImport(ctx, coordinator, plan, false, err)
	}

	var (
		b            = newImportBatch()
		totalUpdates = 0
	)
	fmt.Printf("max-workers=%d\n", maxWorkers)
	steps := buildImportSteps(
		ctx,
		config,
		plan,
		b,
		chunk,
		maxWorkers,
		database.DB,
		preparation.RunID,
		preparation.ResumedFromRunID != 0,
	)

	for i := range steps {
		r := steps[i]()
		totalUpdates += r.Count()
		if r.IsErr() {
			err = r.Err()
			break
		}
	}

	err = finalizeImport(ctx, coordinator, plan, config.Bool("cleanup"), err)
	printResult(begin, totalUpdates, err)
	return err
}

func finalizeImport(
	ctx context.Context,
	completer importCompleter,
	plan *data.ImportPlan,
	cleanup bool,
	runErr error,
) error {
	completionCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		importCompletionTimeout,
	)
	defer cancel()
	completionErr := completer.Complete(completionCtx, runErr)
	finalErr := errors.Join(runErr, completionErr)
	if finalErr != nil || !cleanup {
		return finalErr
	}
	return cleanupImportFiles(plan)
}

func preloadReferenceIDs(ctx context.Context, db *sql.DB, config *koanf.Koanf) error {
	loads := []struct {
		required bool
		table    string
		target   *cache.IDSet
	}{
		{
			required: (hasMaster(config) || hasRelease(config)) && !hasArtist(config),
			table:    "artist",
			target:   cache.ArtistIDs,
		},
		{
			required: hasRelease(config) && !hasLabel(config),
			table:    "label",
			target:   cache.LabelIDs,
		},
		{
			required: hasRelease(config) && !hasMaster(config),
			table:    "master",
			target:   cache.MasterIDs,
		},
	}
	for _, load := range loads {
		if !load.required {
			continue
		}
		rows, err := db.QueryContext(ctx, "SELECT id FROM "+load.table)
		if err != nil {
			return fmt.Errorf("stream %s identifiers: %w", load.table, err)
		}
		var count int64
		for rows.Next() {
			var id int32
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan %s identifier: %w", load.table, err)
			}
			load.target.Add(id)
			count++
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read %s identifiers: %w", load.table, err)
		}
		if err := closeIdentifierRows(rows); err != nil {
			return fmt.Errorf("close %s identifier stream: %w", load.table, err)
		}
		fmt.Printf("cached %d %s identifiers\n", count, load.table)
	}
	return nil
}

func buildImportSteps(
	ctx context.Context,
	config *koanf.Koanf,
	plan *data.ImportPlan,
	batch Batch,
	chunkSize int,
	maxWorkers int,
	db *gorm.DB,
	runID int64,
	resume bool,
) []Step {
	definitions := []struct {
		enabled     bool
		entityType  string
		resourceKey string
		build       func(Order) Step
	}{
		{hasArtist(config), "artist", "artists", batch.UpdateArtist},
		{hasLabel(config), "label", "labels", batch.UpdateLabel},
		{hasMaster(config), "master", "masters", batch.UpdateMaster},
		{hasRelease(config), "release", "releases", batch.UpdateRelease},
	}
	steps := make([]Step, 0, len(definitions))
	for _, definition := range definitions {
		if !definition.enabled {
			continue
		}
		order := NewTrackedOrder(
			ctx,
			chunkSize,
			maxWorkers,
			plan.Resources[definition.resourceKey],
			db,
			runID,
			definition.entityType,
			resume,
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
