package testutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/moby/moby/api/types/mount"
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

func setupPostgres(t testing.TB) Database {
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
	dbContainer, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
	if err != nil {
		t.Fatalf("start PostgreSQL test container: %v", err)
	}
	t.Cleanup(func() {
		if terminateErr := dbContainer.Terminate(context.Background()); terminateErr != nil {
			t.Errorf("terminate PostgreSQL test container: %v", terminateErr)
		}
	})

	inspection, err := dbContainer.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect PostgreSQL test container: %v", err)
	}
	for _, mounted := range inspection.Mounts {
		if mounted.Type == mount.TypeVolume {
			t.Fatalf("PostgreSQL test container must not create volume %q at %q", mounted.Name, mounted.Destination)
		}
	}

	host, err := dbContainer.Host(ctx)
	if err != nil {
		t.Fatalf("resolve PostgreSQL test container host: %v", err)
	}
	port, err := dbContainer.MappedPort(ctx, postgresPort)
	if err != nil {
		t.Fatalf("resolve PostgreSQL test container port: %v", err)
	}
	return Database{
		Username: postgresUsername,
		Password: postgresPassword,
		Hostname: host,
		DBName:   postgresDatabase,
		Type:     Postgres,
		Port:     port.Port(),
	}
}
