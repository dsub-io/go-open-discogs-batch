package testutils

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	postgresImage         = "postgres:18.4-alpine"
	postgresPassword      = "postgres"
	postgresPort          = "5432/tcp"
	postgresUsername      = "postgres"
	postgresWaitLog       = `database system is ready to accept connections`
	testOwnerLabelKey     = "io.dsub.test-owner"
	testOwnerLabelValue   = "go-open-discogs-batch"
	postgresDataDirectory = "/var/lib/postgresql"
)

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

var createPostgresContainer = startPostgresContainer

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
		return setupPostgres(t)
	}
	panic("unsupported database type")
}

func setupPostgres(t testReporter) Database {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
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
		},
		Tmpfs: map[string]string{
			postgresDataDirectory: "rw,noexec,nosuid,size=512m",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog(postgresWaitLog),
			wait.ForExposedPort().WithStartupTimeout(time.Second*180),
			wait.ForListeningPort(postgresPort).WithStartupTimeout(10*time.Second),
		).WithDeadline(time.Second * 120),
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
