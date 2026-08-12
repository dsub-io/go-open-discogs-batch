package batch

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/data"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type executionCoordinatorStub struct {
	preparation *ImportPreparation
	prepareErr  error
	completeErr error
}

func (s *executionCoordinatorStub) Prepare(
	context.Context,
	[]*opendiscogsmodel.DiscogsDump,
	int,
	bool,
	bool,
) (*ImportPreparation, error) {
	return s.preparation, s.prepareErr
}

func (s *executionCoordinatorStub) Complete(context.Context, error) error {
	return s.completeErr
}

type batchStub struct {
	stepResult result.Result
}

func (s batchStub) step(Order) Step                { return func() result.Result { return s.stepResult } }
func (s batchStub) UpdateArtist(order Order) Step  { return s.step(order) }
func (s batchStub) UpdateLabel(order Order) Step   { return s.step(order) }
func (s batchStub) UpdateMaster(order Order) Step  { return s.step(order) }
func (s batchStub) UpdateRelease(order Order) Step { return s.step(order) }

type runnerSeams struct {
	connect       func(string, string) error
	configure     func(*gorm.DB, int) error
	ensure        func(*gorm.DB, database.Schema) (bool, error)
	ddl           func(*gorm.DB, string) error
	refresh       func(context.Context, data.Repository, []string, string) (int, error)
	resolve       func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error)
	fetch         func(context.Context, *data.ImportPlan) error
	open          func(*gorm.DB) (*sql.DB, error)
	preflight     func(context.Context, *sql.DB, []*opendiscogsmodel.DiscogsDump) error
	coordinator   func(*sql.DB, string) importExecutionCoordinator
	preload       func(context.Context, *sql.DB, *koanf.Koanf) error
	newBatch      func() Batch
	databaseValue *gorm.DB
}

func installRunnerSeams(t *testing.T, seams runnerSeams) {
	t.Helper()
	originalConnect := connectDatabase
	originalConfigure := configureDatabasePool
	originalEnsure := ensureDatabaseSchema
	originalDDL := runDatabaseDDL
	originalRefresh := refreshSelectedData
	originalResolve := resolveImportPlan
	originalFetch := fetchImportResources
	originalOpen := openSQLDatabase
	originalPreflight := validateImportDependencies
	originalCoordinator := newExecutionCoordinator
	originalPreload := preloadImportReferenceIDs
	originalBatch := newImportBatch
	originalDB := database.DB
	t.Cleanup(func() {
		connectDatabase = originalConnect
		configureDatabasePool = originalConfigure
		ensureDatabaseSchema = originalEnsure
		runDatabaseDDL = originalDDL
		refreshSelectedData = originalRefresh
		resolveImportPlan = originalResolve
		fetchImportResources = originalFetch
		openSQLDatabase = originalOpen
		validateImportDependencies = originalPreflight
		newExecutionCoordinator = originalCoordinator
		preloadImportReferenceIDs = originalPreload
		newImportBatch = originalBatch
		database.DB = originalDB
	})
	connectDatabase = seams.connect
	configureDatabasePool = seams.configure
	ensureDatabaseSchema = seams.ensure
	runDatabaseDDL = seams.ddl
	refreshSelectedData = seams.refresh
	resolveImportPlan = seams.resolve
	fetchImportResources = seams.fetch
	openSQLDatabase = seams.open
	validateImportDependencies = seams.preflight
	newExecutionCoordinator = seams.coordinator
	preloadImportReferenceIDs = seams.preload
	newImportBatch = seams.newBatch
	database.DB = seams.databaseValue
}

func defaultRunnerSeams() runnerSeams {
	coordinator := &executionCoordinatorStub{preparation: &ImportPreparation{RunID: 1}}
	return runnerSeams{
		connect:   func(string, string) error { return nil },
		configure: func(*gorm.DB, int) error { return nil },
		ensure:    func(*gorm.DB, database.Schema) (bool, error) { return false, nil },
		ddl:       func(*gorm.DB, string) error { return nil },
		refresh: func(context.Context, data.Repository, []string, string) (int, error) {
			return 0, nil
		},
		resolve: func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) {
			return &data.ImportPlan{
				Resources: map[string]string{"artists": "unused"},
				Dumps:     []*opendiscogsmodel.DiscogsDump{importDump("artist", "2026-07-01", "a")},
			}, nil
		},
		fetch: func(context.Context, *data.ImportPlan) error { return nil },
		open:  func(*gorm.DB) (*sql.DB, error) { return nil, nil },
		preflight: func(
			context.Context,
			*sql.DB,
			[]*opendiscogsmodel.DiscogsDump,
		) error {
			return nil
		},
		coordinator: func(*sql.DB, string) importExecutionCoordinator { return coordinator },
		preload:     func(context.Context, *sql.DB, *koanf.Koanf) error { return nil },
		newBatch: func() Batch {
			return batchStub{stepResult: result.NewResult(1, nil)}
		},
	}
}

func runnerConfig(t *testing.T) *koanf.Koanf {
	t.Helper()
	config := koanf.New(".")
	for key, value := range map[string]interface{}{
		"entities":        []string{"artist"},
		"artists":         true,
		"chunk-size":      1,
		"max-workers":     1,
		"database-schema": database.DefaultSchemaName,
	} {
		require.NoError(t, config.Set(key, value))
	}
	return config
}

func TestRunnerPropagatesEveryStageFailure(t *testing.T) {
	expected := errors.New("fixture")
	tests := []struct {
		name   string
		mutate func(*runnerSeams)
	}{
		{"connect", func(s *runnerSeams) { s.connect = func(string, string) error { return expected } }},
		{"pool", func(s *runnerSeams) { s.configure = func(*gorm.DB, int) error { return expected } }},
		{"schema", func(s *runnerSeams) {
			s.ensure = func(*gorm.DB, database.Schema) (bool, error) { return false, expected }
		}},
		{"ddl", func(s *runnerSeams) { s.ddl = func(*gorm.DB, string) error { return expected } }},
		{"catalog and plan", func(s *runnerSeams) {
			s.refresh = func(context.Context, data.Repository, []string, string) (int, error) { return 0, expected }
			s.resolve = func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) { return nil, expected }
		}},
		{"open SQL pool", func(s *runnerSeams) { s.open = func(*gorm.DB) (*sql.DB, error) { return nil, expected } }},
		{"dependency preflight", func(s *runnerSeams) {
			s.preflight = func(
				context.Context,
				*sql.DB,
				[]*opendiscogsmodel.DiscogsDump,
			) error {
				return expected
			}
		}},
		{"prepare", func(s *runnerSeams) {
			s.coordinator = func(*sql.DB, string) importExecutionCoordinator {
				return &executionCoordinatorStub{prepareErr: expected}
			}
		}},
		{"fetch", func(s *runnerSeams) { s.fetch = func(context.Context, *data.ImportPlan) error { return expected } }},
		{"preload", func(s *runnerSeams) {
			s.preload = func(context.Context, *sql.DB, *koanf.Koanf) error { return expected }
		}},
		{"step", func(s *runnerSeams) {
			s.newBatch = func() Batch { return batchStub{stepResult: result.NewResult(0, expected)} }
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seams := defaultRunnerSeams()
			test.mutate(&seams)
			installRunnerSeams(t, seams)
			require.ErrorIs(t, (&Runner{Version: "test"}).Run(context.Background(), runnerConfig(t)), expected)
		})
	}
}

func TestRunnerRechecksDependenciesAfterAdmissionBeforeFetch(t *testing.T) {
	expected := errors.New("dependency changed during admission")
	seams := defaultRunnerSeams()
	preflightCalls := 0
	fetchCalled := false
	seams.preflight = func(
		context.Context,
		*sql.DB,
		[]*opendiscogsmodel.DiscogsDump,
	) error {
		preflightCalls++
		if preflightCalls == 2 {
			return expected
		}
		return nil
	}
	seams.fetch = func(context.Context, *data.ImportPlan) error {
		fetchCalled = true
		return nil
	}
	installRunnerSeams(t, seams)

	err := (&Runner{Version: "test"}).Run(context.Background(), runnerConfig(t))

	require.ErrorIs(t, err, expected)
	require.Equal(t, 2, preflightCalls)
	require.False(t, fetchCalled)
}

func TestRunnerRejectsInvalidDatabaseSchema(t *testing.T) {
	config := runnerConfig(t)
	require.NoError(t, config.Set("database-schema", "Invalid"))

	require.ErrorContains(
		t,
		(&Runner{Version: "test"}).Run(context.Background(), config),
		"database-schema",
	)
}

func TestBuildImportStepsSkipsDisabledEntitiesAndPrintsError(t *testing.T) {
	config := koanf.New(".")
	require.Empty(t, buildImportSteps(
		context.Background(),
		config,
		&data.ImportPlan{Resources: map[string]string{}},
		batchStub{stepResult: result.NewResult(0, nil)},
		1,
		1,
		nil,
		1,
		false,
	))
	printResult(time.Now(), 1, errors.New("fixture"))
}
