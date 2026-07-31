package database

import (
	"errors"
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
	} else if sqlDB, sqlDbErr := DB.DB(); sqlDbErr != nil {
		return sqlDbErr
	} else {
		sqlDB.SetConnMaxLifetime(time.Second)
		return
	}
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

	return gorm.Open(dl, &gorm.Config{Logger: newLogger})
}
