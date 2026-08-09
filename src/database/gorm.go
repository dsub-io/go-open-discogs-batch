package database

import (
	"errors"
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"regexp"
	"time"
)

var (
	p = regexp.MustCompile(`(^(postgres|postgresql)://.*$|^host=\w+ user=\w+ password=\w+ dbname=\w+ port=\d+ .*$)`)
	x = regexp.MustCompile(`^(.*)://.*$`)
)

var DB *gorm.DB

// Connect opens the canonical PostgreSQL database.
func Connect(dsn string) (err error) {
	if DB, err = GetConnect(dsn); err != nil {
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
	var dl gorm.Dialector
	if len(dsn) == 0 {
		return nil, errors.New("missing dsn")
	} else if p.MatchString(dsn) {
		dl = postgres.Open(dsn)
	} else {
		if match := x.FindStringSubmatch(dsn); match != nil {
			return nil, errors.New("unsupported database from dsn: " + match[1])
		} else {
			return nil, errors.New("unsupported dsn. please check again")
		}
	}

	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             time.Second,
			Colorful:                  true,
			IgnoreRecordNotFoundError: false,
			LogLevel:                  logger.Error,
		})

	return gorm.Open(dl, &gorm.Config{
		Logger:                 newLogger,
		SkipDefaultTransaction: true,
	})
}
