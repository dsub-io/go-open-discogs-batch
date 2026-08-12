package database

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	p = regexp.MustCompile(`(^(postgres|postgresql)://.*$|^host=\w+ user=\w+ password=\w+ dbname=\w+ port=\d+ .*$)`)
	x = regexp.MustCompile(`^(.*)://.*$`)
)

var DB *gorm.DB

// Connect opens the canonical PostgreSQL database.
func Connect(dsn string) (err error) {
	return ConnectInSchema(dsn, DefaultSchemaName)
}

// ConnectInSchema opens PostgreSQL with the selected schema first in every connection's search path.
func ConnectInSchema(dsn, schemaName string) (err error) {
	if DB, err = GetConnectInSchema(dsn, schemaName); err != nil {
		return
	}
	return nil
}

// ConfigurePool reserves one connection for the import coordinator and bounds all remaining
// connections by the configured worker limit. Connections are not forcibly recycled during a long
// import; only idle connections expire.
func ConfigurePool(db *gorm.DB, maxWorkers int) error {
	if maxWorkers < 1 {
		return fmt.Errorf("max workers must be positive")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("open SQL connection pool: %w", err)
	}
	poolSize := maxWorkers + 1
	sqlDB.SetMaxOpenConns(poolSize)
	sqlDB.SetMaxIdleConns(poolSize)
	sqlDB.SetConnMaxLifetime(0)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	return nil
}

func GetConnect(dsn string) (*gorm.DB, error) {
	return GetConnectInSchema(dsn, DefaultSchemaName)
}

// GetConnectInSchema creates a bounded-schema GORM connection without mutating the caller's DSN.
func GetConnectInSchema(dsn, schemaName string) (*gorm.DB, error) {
	if len(dsn) == 0 {
		return nil, errors.New("missing dsn")
	}
	if !p.MatchString(dsn) {
		if match := x.FindStringSubmatch(dsn); match != nil {
			return nil, errors.New("unsupported database from dsn: " + match[1])
		}
		return nil, errors.New("unsupported dsn. please check again")
	}
	schema, err := ParseSchema(schemaName)
	if err != nil {
		return nil, err
	}
	connectionConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL DSN: %w", err)
	}
	connectionConfig.RuntimeParams[searchPathParameter] = schema.SearchPath()
	sqlDB := stdlib.OpenDB(*connectionConfig)

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{
		Logger:                 logger.Discard,
		SkipDefaultTransaction: true,
	})
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return db, nil
}
