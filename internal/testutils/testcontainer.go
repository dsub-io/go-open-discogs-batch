package testutils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	containerapi "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type DatabaseType int

const (
	Postgres DatabaseType = iota + 1
)

const (
	postgresDatabase      = "test_db"
	postgresTestDBPrefix  = "test_db_case"
	postgresImage         = "postgres:18.4-alpine"
	postgresPassword      = "postgres"
	postgresPort          = "5432/tcp"
	postgresUsername      = "postgres"
	postgresWaitLog       = `database system is ready to accept connections`
	testOwnerLabelKey     = "io.dsub.test-owner"
	testOwnerLabelValue   = "go-open-discogs-batch"
	testRunIDEnv          = "OPEN_DISCOGS_TEST_RUN_ID"
	testRunIDLabelKey     = "io.dsub.test-run"
	testRunIDLocalPrefix  = "local"
	testRunIDMaxLength    = 128
	postgresDataDirectory = "/var/lib/postgresql"
)

var (
	createPostgresContainer = startPostgresContainer
	localTestRunID          = fmt.Sprintf(
		"%s-%d-%d",
		testRunIDLocalPrefix,
		os.Getpid(),
		time.Now().UTC().UnixNano(),
	)
	testRunIDPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	sharedPostgres         = new(postgresFixture)
	ensurePostgresDatabase = ensureSharedPostgres
	executePostgresDDL     = executeDatabaseDDL
)

type postgresFixture struct {
	sync.Mutex
	container postgresContainer
	database  Database
	nextID    uint64
}

type Database struct {
	Username string
	Password string
	Hostname string
	DBName   string
	Type     DatabaseType
	Port     string
}

type testReporter interface {
	Helper()
	Cleanup(func())
	Fatalf(string, ...interface{})
	Errorf(string, ...interface{})
}

type postgresContainer interface {
	Terminate(context.Context, ...testcontainers.TerminateOption) error
	Inspect(context.Context) (*containerapi.InspectResponse, error)
	Host(context.Context) (string, error)
	MappedPort(context.Context, string) (network.Port, error)
}

func startPostgresContainer(
	ctx context.Context,
	req testcontainers.ContainerRequest,
) (postgresContainer, error) {
	return testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		},
	)
}

func GetDsn(dt DatabaseType, db Database) string {
	if dt != Postgres {
		panic("unsupported database type")
	}
	return fmt.Sprintf(
		"host=%+v user=%+v password=%+v dbname=%+v port=%+v sslmode=disable",
		db.Hostname,
		db.Username,
		db.Password,
		db.DBName,
		db.Port,
	)
}

func GetDatabase(t testing.TB, db DatabaseType) Database {
	t.Helper()
	if db == Postgres {
		return isolatedPostgresDatabase(t)
	}
	panic("unsupported database type")
}

func isolatedPostgresDatabase(t testReporter) Database {
	t.Helper()
	base, err := ensurePostgresDatabase()
	if err != nil {
		t.Fatalf("start shared PostgreSQL test container: %v", err)
		return Database{}
	}

	sharedPostgres.Lock()
	sharedPostgres.nextID++
	databaseName := fmt.Sprintf(
		"%s_%d_%d",
		postgresTestDBPrefix,
		os.Getpid(),
		sharedPostgres.nextID,
	)
	sharedPostgres.Unlock()
	if err := executePostgresDDL(base, "create database "+pgx.Identifier{databaseName}.Sanitize()); err != nil {
		t.Fatalf("create isolated PostgreSQL test database %q: %v", databaseName, err)
		return Database{}
	}
	t.Cleanup(func() {
		statement := "drop database if exists " + pgx.Identifier{databaseName}.Sanitize() + " with (force)"
		if err := executePostgresDDL(base, statement); err != nil {
			t.Errorf("drop isolated PostgreSQL test database %q: %v", databaseName, err)
		}
	})
	database := base
	database.DBName = databaseName
	return database
}

func ensureSharedPostgres() (Database, error) {
	return sharedPostgres.ensure(postgresContainerRequest, createPostgresContainer)
}

func (fixture *postgresFixture) ensure(
	request func() (testcontainers.ContainerRequest, error),
	create func(context.Context, testcontainers.ContainerRequest) (postgresContainer, error),
) (Database, error) {
	fixture.Lock()
	defer fixture.Unlock()
	if fixture.container != nil {
		return fixture.database, nil
	}
	containerRequest, err := request()
	if err != nil {
		return Database{}, err
	}
	ctx := context.Background()
	container, err := create(ctx, containerRequest)
	if err != nil {
		return Database{}, err
	}
	database, err := databaseFromContainer(ctx, container)
	if err != nil {
		return Database{}, errors.Join(err, container.Terminate(ctx))
	}
	fixture.container = container
	fixture.database = database
	return database, nil
}

// StopSharedPostgres releases the package-scoped PostgreSQL fixture.
func StopSharedPostgres() error {
	return sharedPostgres.stop()
}

func (fixture *postgresFixture) stop() error {
	fixture.Lock()
	defer fixture.Unlock()
	if fixture.container == nil {
		return nil
	}
	err := fixture.container.Terminate(context.Background())
	fixture.container = nil
	fixture.database = Database{}
	fixture.nextID = 0
	return err
}

func executeDatabaseDDL(database Database, statement string) error {
	ctx := context.Background()
	connection, err := pgx.Connect(ctx, GetDsn(Postgres, database))
	if err != nil {
		return err
	}
	defer connection.Close(ctx)
	_, err = connection.Exec(ctx, statement)
	return err
}

func setupPostgres(t testReporter) Database {
	t.Helper()
	ctx := context.Background()
	req, err := postgresContainerRequest()
	if err != nil {
		t.Fatalf("configure PostgreSQL test container: %v", err)
		return Database{}
	}
	dbContainer, err := createPostgresContainer(ctx, req)
	if err != nil {
		t.Fatalf("start PostgreSQL test container: %v", err)
		return Database{}
	}
	t.Cleanup(func() {
		reportPostgresTermination(t, dbContainer)
	})

	database, err := databaseFromContainer(ctx, dbContainer)
	if err != nil {
		t.Fatalf("resolve PostgreSQL test container: %v", err)
		return Database{}
	}
	return database
}

func postgresContainerRequest() (testcontainers.ContainerRequest, error) {
	testRunID, err := CurrentTestRunID()
	if err != nil {
		return testcontainers.ContainerRequest{}, err
	}
	return testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{},
		Image:          postgresImage,
		Entrypoint:     nil,
		Env: map[string]string{
			"POSTGRES_DB":       postgresDatabase,
			"POSTGRES_PASSWORD": postgresPassword,
			"POSTGRES_USER":     postgresUsername,
		},
		ExposedPorts: []string{postgresPort},
		Labels: map[string]string{
			testOwnerLabelKey: testOwnerLabelValue,
			testRunIDLabelKey: testRunID,
		},
		Tmpfs: map[string]string{
			postgresDataDirectory: "rw,noexec,nosuid,size=512m",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog(postgresWaitLog),
			wait.ForExposedPort().WithStartupTimeout(time.Second*180),
			wait.ForListeningPort(postgresPort).WithStartupTimeout(10*time.Second),
		).WithDeadline(time.Second * 120),
	}, nil
}

// CurrentTestRunID identifies resources owned by the current test invocation.
func CurrentTestRunID() (string, error) {
	testRunID := os.Getenv(testRunIDEnv)
	if testRunID == "" {
		testRunID = localTestRunID
	}
	if len(testRunID) > testRunIDMaxLength || !testRunIDPattern.MatchString(testRunID) {
		return "", fmt.Errorf(
			"%s must be 1-%d characters using letters, digits, dot, underscore, or hyphen",
			testRunIDEnv,
			testRunIDMaxLength,
		)
	}
	return testRunID, nil
}

func reportPostgresTermination(t testReporter, dbContainer postgresContainer) {
	if err := dbContainer.Terminate(context.Background()); err != nil {
		t.Errorf("terminate PostgreSQL test container: %v", err)
	}
}

func databaseFromContainer(ctx context.Context, dbContainer postgresContainer) (Database, error) {
	inspection, err := dbContainer.Inspect(ctx)
	if err != nil {
		return Database{}, fmt.Errorf("inspect PostgreSQL test container: %w", err)
	}
	for _, mounted := range inspection.Mounts {
		if mounted.Type == mount.TypeVolume {
			return Database{}, fmt.Errorf(
				"PostgreSQL test container must not create volume %q at %q",
				mounted.Name,
				mounted.Destination,
			)
		}
	}

	host, err := dbContainer.Host(ctx)
	if err != nil {
		return Database{}, fmt.Errorf("resolve PostgreSQL test container host: %w", err)
	}
	port, err := dbContainer.MappedPort(ctx, postgresPort)
	if err != nil {
		return Database{}, fmt.Errorf("resolve PostgreSQL test container port: %w", err)
	}
	return Database{
		Username: postgresUsername,
		Password: postgresPassword,
		Hostname: host,
		DBName:   postgresDatabase,
		Type:     Postgres,
		Port:     port.Port(),
	}, nil
}
