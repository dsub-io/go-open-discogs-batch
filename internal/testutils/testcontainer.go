package testutils

import (
	"context"
	"fmt"

	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"time"
)

type DatabaseType int

const (
	Postgres DatabaseType = iota + 1
)

const postgresWaitLog = `database system is ready to accept connections`

type Database struct {
	Username  string
	Password  string
	Hostname  string
	DBName    string
	Type      DatabaseType
	Port      string
	Container testcontainers.Container
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

func GetDatabase(db DatabaseType) Database {
	if db == Postgres {
		return setupPostgres()
	}
	panic("unsupported database type")
}

func setupPostgres() Database {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{},
		Image:          "postgres:latest",
		Entrypoint:     nil,
		Env: map[string]string{
			"POSTGRES_DB":       "test_db",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_USER":     "postgres",
		},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForLog(postgresWaitLog),
			wait.ForExposedPort().WithStartupTimeout(time.Second*180),
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(10*time.Second),
		).WithDeadline(time.Second * 120),
	}
	dbContainer, err := testcontainers.GenericContainer(
		context.Background(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})

	if err != nil {
		panic(err)
	}

	host, _ := dbContainer.Host(context.Background())
	port, _ := dbContainer.MappedPort(context.Background(), "5432")
	return Database{
		Username:  "postgres",
		Password:  "postgres",
		Hostname:  host,
		DBName:    "test_db",
		Type:      Postgres,
		Port:      port.Port(),
		Container: dbContainer,
	}
}
