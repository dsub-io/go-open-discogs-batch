package testutils

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/dsub-io/go-open-discogs-batch/src/database"
	containerapi "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
)

func TestDatabaseHelpers(t *testing.T) {
	databaseConfig := GetDatabase(t, Postgres)
	secondDatabase := GetDatabase(t, Postgres)
	require.Equal(t, databaseConfig.Hostname, secondDatabase.Hostname)
	require.Equal(t, databaseConfig.Port, secondDatabase.Port)
	require.NotEqual(t, databaseConfig.DBName, secondDatabase.DBName)
	dsn := GetDsn(Postgres, databaseConfig)

	require.Contains(t, dsn, "host="+databaseConfig.Hostname)
	require.Contains(t, dsn, "dbname="+databaseConfig.DBName)
	require.Panics(t, func() { GetDsn(DatabaseType(99), databaseConfig) })
	require.Panics(t, func() { GetDatabase(t, DatabaseType(99)) })

	db, err := database.GetConnect(dsn)
	require.NoError(t, err)
	require.NoError(t, ApplySharedSchema(db))
}

func TestPostgresFixtureLifecycleBoundaries(t *testing.T) {
	expected := errors.New("fixture")
	request := func() (testcontainers.ContainerRequest, error) {
		return testcontainers.ContainerRequest{}, nil
	}

	t.Run("cached", func(t *testing.T) {
		database := Database{DBName: "cached"}
		fixture := &postgresFixture{container: validPostgresContainerStub(), database: database}
		actual, err := fixture.ensure(
			func() (testcontainers.ContainerRequest, error) {
				t.Fatal("cached fixture must not rebuild its request")
				return testcontainers.ContainerRequest{}, nil
			},
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, database, actual)
	})

	t.Run("request failure", func(t *testing.T) {
		fixture := new(postgresFixture)
		_, err := fixture.ensure(
			func() (testcontainers.ContainerRequest, error) {
				return testcontainers.ContainerRequest{}, expected
			},
			nil,
		)
		require.ErrorIs(t, err, expected)
	})

	t.Run("create failure", func(t *testing.T) {
		fixture := new(postgresFixture)
		_, err := fixture.ensure(
			request,
			func(context.Context, testcontainers.ContainerRequest) (postgresContainer, error) {
				return nil, expected
			},
		)
		require.ErrorIs(t, err, expected)
	})

	t.Run("resolution failure terminates container", func(t *testing.T) {
		fixture := new(postgresFixture)
		container := validPostgresContainerStub()
		container.inspectErr = expected
		container.terminateErr = errors.New("termination fixture")
		_, err := fixture.ensure(
			request,
			func(context.Context, testcontainers.ContainerRequest) (postgresContainer, error) {
				return container, nil
			},
		)
		require.ErrorContains(t, err, expected.Error())
		require.ErrorContains(t, err, container.terminateErr.Error())
		require.Nil(t, fixture.container)
	})

	t.Run("start and stop", func(t *testing.T) {
		fixture := new(postgresFixture)
		container := validPostgresContainerStub()
		database, err := fixture.ensure(
			request,
			func(context.Context, testcontainers.ContainerRequest) (postgresContainer, error) {
				return container, nil
			},
		)
		require.NoError(t, err)
		require.Equal(t, postgresDatabase, database.DBName)
		require.NoError(t, fixture.stop())
		require.Nil(t, fixture.container)
		require.Equal(t, Database{}, fixture.database)
		require.Zero(t, fixture.nextID)
		require.NoError(t, fixture.stop())
	})

	t.Run("stop reports termination failure and resets state", func(t *testing.T) {
		container := validPostgresContainerStub()
		container.terminateErr = expected
		fixture := &postgresFixture{
			container: container,
			database:  Database{DBName: "fixture"},
			nextID:    3,
		}
		require.ErrorIs(t, fixture.stop(), expected)
		require.Nil(t, fixture.container)
		require.Equal(t, Database{}, fixture.database)
		require.Zero(t, fixture.nextID)
	})
}

func TestIsolatedPostgresDatabaseFailureBoundaries(t *testing.T) {
	originalEnsure := ensurePostgresDatabase
	originalDDL := executePostgresDDL
	originalShared := sharedPostgres
	t.Cleanup(func() {
		ensurePostgresDatabase = originalEnsure
		executePostgresDDL = originalDDL
		sharedPostgres = originalShared
	})
	sharedPostgres = new(postgresFixture)
	expected := errors.New("fixture")

	reporter := &reporterStub{}
	ensurePostgresDatabase = func() (Database, error) { return Database{}, expected }
	require.Equal(t, Database{}, isolatedPostgresDatabase(reporter))
	require.Len(t, reporter.fatals, 1)
	require.Empty(t, reporter.cleanups)

	reporter = &reporterStub{}
	base := Database{DBName: postgresDatabase}
	ensurePostgresDatabase = func() (Database, error) { return base, nil }
	executePostgresDDL = func(Database, string) error { return expected }
	require.Equal(t, Database{}, isolatedPostgresDatabase(reporter))
	require.Len(t, reporter.fatals, 1)
	require.Empty(t, reporter.cleanups)

	reporter = &reporterStub{}
	calls := 0
	executePostgresDDL = func(Database, string) error {
		calls++
		if calls == 1 {
			return nil
		}
		return expected
	}
	actual := isolatedPostgresDatabase(reporter)
	require.NotEqual(t, postgresDatabase, actual.DBName)
	require.Len(t, reporter.cleanups, 1)
	reporter.cleanups[0]()
	require.Len(t, reporter.errors, 1)
}

func TestStopSharedPostgresDelegatesToCurrentFixture(t *testing.T) {
	original := sharedPostgres
	t.Cleanup(func() { sharedPostgres = original })
	sharedPostgres = new(postgresFixture)
	require.NoError(t, StopSharedPostgres())
}

func TestExecuteDatabaseDDLReportsConnectionAndStatementFailures(t *testing.T) {
	require.Error(t, executeDatabaseDDL(Database{
		Username: "invalid", Password: "invalid", Hostname: "127.0.0.1",
		DBName: "invalid", Type: Postgres, Port: "1",
	}, "select 1"))

	databaseConfig := GetDatabase(t, Postgres)
	require.Error(t, executeDatabaseDDL(databaseConfig, "invalid SQL"))
}

type reporterStub struct {
	cleanups []func()
	fatals   []string
	errors   []string
}

func (*reporterStub) Helper()                  {}
func (r *reporterStub) Cleanup(cleanup func()) { r.cleanups = append(r.cleanups, cleanup) }
func (r *reporterStub) Fatalf(format string, args ...interface{}) {
	r.fatals = append(r.fatals, format)
}
func (r *reporterStub) Errorf(format string, args ...interface{}) {
	r.errors = append(r.errors, format)
}

type postgresContainerStub struct {
	inspection   *containerapi.InspectResponse
	inspectErr   error
	host         string
	hostErr      error
	port         network.Port
	portErr      error
	terminateErr error
}

func (c *postgresContainerStub) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return c.terminateErr
}
func (c *postgresContainerStub) Inspect(context.Context) (*containerapi.InspectResponse, error) {
	return c.inspection, c.inspectErr
}
func (c *postgresContainerStub) Host(context.Context) (string, error) {
	return c.host, c.hostErr
}
func (c *postgresContainerStub) MappedPort(context.Context, string) (network.Port, error) {
	return c.port, c.portErr
}

func validPostgresContainerStub() *postgresContainerStub {
	return &postgresContainerStub{
		inspection: &containerapi.InspectResponse{},
		host:       "127.0.0.1",
		port:       network.MustParsePort("15432/tcp"),
	}
}

func TestCurrentTestRunID(t *testing.T) {
	t.Setenv(testRunIDEnv, "")
	actual, err := CurrentTestRunID()
	require.NoError(t, err)
	require.Equal(t, localTestRunID, actual)

	t.Setenv(testRunIDEnv, "ci-123.4_job")
	actual, err = CurrentTestRunID()
	require.NoError(t, err)
	require.Equal(t, "ci-123.4_job", actual)

	for _, invalid := range []string{
		"contains/slash",
		" contains-space",
		strings.Repeat("a", testRunIDMaxLength+1),
	} {
		t.Setenv(testRunIDEnv, invalid)
		_, err = CurrentTestRunID()
		require.ErrorContains(t, err, testRunIDEnv)
	}
}

func TestSetupPostgresAppliesExactOwnershipLabels(t *testing.T) {
	originalCreate := createPostgresContainer
	t.Cleanup(func() { createPostgresContainer = originalCreate })
	t.Setenv(testRunIDEnv, "focused-test-run")
	reporter := &reporterStub{}

	createPostgresContainer = func(
		_ context.Context,
		req testcontainers.ContainerRequest,
	) (postgresContainer, error) {
		require.Equal(t, testOwnerLabelValue, req.Labels[testOwnerLabelKey])
		require.Equal(t, "focused-test-run", req.Labels[testRunIDLabelKey])
		return validPostgresContainerStub(), nil
	}

	require.NotEqual(t, Database{}, setupPostgres(reporter))
	require.Empty(t, reporter.fatals)
	require.Len(t, reporter.cleanups, 1)
	reporter.cleanups[0]()
}

func TestSetupPostgresRejectsInvalidRunIdentityBeforeStartingContainer(t *testing.T) {
	originalCreate := createPostgresContainer
	t.Cleanup(func() { createPostgresContainer = originalCreate })
	t.Setenv(testRunIDEnv, "invalid/run")
	reporter := &reporterStub{}
	started := false
	createPostgresContainer = func(
		context.Context,
		testcontainers.ContainerRequest,
	) (postgresContainer, error) {
		started = true
		return validPostgresContainerStub(), nil
	}

	require.Equal(t, Database{}, setupPostgres(reporter))
	require.False(t, started)
	require.Len(t, reporter.fatals, 1)
	require.Empty(t, reporter.cleanups)
}

func TestSetupPostgresReportsStartAndResolutionFailures(t *testing.T) {
	originalCreate := createPostgresContainer
	t.Cleanup(func() { createPostgresContainer = originalCreate })
	reporter := &reporterStub{}
	expected := errors.New("fixture")

	createPostgresContainer = func(context.Context, testcontainers.ContainerRequest) (postgresContainer, error) {
		return nil, expected
	}
	require.Equal(t, Database{}, setupPostgres(reporter))
	require.Len(t, reporter.fatals, 1)

	reporter = &reporterStub{}
	container := validPostgresContainerStub()
	container.inspectErr = expected
	createPostgresContainer = func(context.Context, testcontainers.ContainerRequest) (postgresContainer, error) {
		return container, nil
	}
	require.Equal(t, Database{}, setupPostgres(reporter))
	require.Len(t, reporter.fatals, 1)
	require.Len(t, reporter.cleanups, 1)
	reporter.cleanups[0]()
}

func TestDatabaseFromContainerErrorBoundaries(t *testing.T) {
	ctx := context.Background()
	expected := errors.New("fixture")

	container := validPostgresContainerStub()
	container.inspectErr = expected
	_, err := databaseFromContainer(ctx, container)
	require.ErrorContains(t, err, "inspect")

	container = validPostgresContainerStub()
	container.inspection.Mounts = []containerapi.MountPoint{{
		Type:        mount.TypeVolume,
		Name:        "unexpected",
		Destination: postgresDataDirectory,
	}}
	_, err = databaseFromContainer(ctx, container)
	require.ErrorContains(t, err, "must not create volume")

	container = validPostgresContainerStub()
	container.hostErr = expected
	_, err = databaseFromContainer(ctx, container)
	require.ErrorContains(t, err, "host")

	container = validPostgresContainerStub()
	container.portErr = expected
	_, err = databaseFromContainer(ctx, container)
	require.ErrorContains(t, err, "port")

	container = validPostgresContainerStub()
	actual, err := databaseFromContainer(ctx, container)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", actual.Hostname)
	require.Equal(t, "15432", actual.Port)
}

func TestReportPostgresTermination(t *testing.T) {
	reporter := &reporterStub{}
	reportPostgresTermination(reporter, validPostgresContainerStub())
	require.Empty(t, reporter.errors)

	container := validPostgresContainerStub()
	container.terminateErr = errors.New("fixture")
	reportPostgresTermination(reporter, container)
	require.Len(t, reporter.errors, 1)
}

func TestApplySharedSchemaErrorBoundaries(t *testing.T) {
	originalLoad := loadSharedMigrations
	t.Cleanup(func() { loadSharedMigrations = originalLoad })
	expected := errors.New("fixture")
	loadSharedMigrations = func() (fs.FS, error) { return nil, expected }
	require.ErrorIs(t, ApplySharedSchema(nil), expected)

	databaseConfig := GetDatabase(t, Postgres)
	db, err := database.GetConnect(GetDsn(Postgres, databaseConfig))
	require.NoError(t, err)

	readFailure := fstest.MapFS{
		"001-broken.sql": &fstest.MapFile{Mode: fs.ModeDir},
	}
	require.Error(t, applySharedMigrations(db, readFailure))

	invalidSQL := fstest.MapFS{
		"001-invalid.sql": &fstest.MapFile{Data: []byte("invalid SQL")},
	}
	require.ErrorContains(t, applySharedMigrations(db, invalidSQL), "apply 001-invalid.sql")
}
