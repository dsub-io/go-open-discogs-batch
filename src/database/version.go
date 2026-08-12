package database

import (
	"fmt"

	"gorm.io/gorm"
)

const (
	canonicalModelVersion             = "v0.2.3"
	postgreSQLServerVersionQuery      = "select current_setting('server_version_num')::integer as server_version_num"
	postgreSQLMajorVersionNumberScale = 10_000
)

type postgreSQLServerVersionNumber int

type postgreSQLServerVersionValidator func(db *gorm.DB) error

const minimumPostgreSQLServerVersion postgreSQLServerVersionNumber = 150_000

type postgreSQLServerVersion struct {
	Number postgreSQLServerVersionNumber `gorm:"column:server_version_num"`
}

func validatePostgreSQLServerVersion(db *gorm.DB) error {
	var version postgreSQLServerVersion
	if err := db.Raw(postgreSQLServerVersionQuery).Scan(&version).Error; err != nil {
		return fmt.Errorf("read PostgreSQL server version: %w", err)
	}
	return requireSupportedPostgreSQLServerVersion(version.Number)
}

func requireSupportedPostgreSQLServerVersion(version postgreSQLServerVersionNumber) error {
	if version >= minimumPostgreSQLServerVersion {
		return nil
	}
	return fmt.Errorf(
		"PostgreSQL %d is unsupported (server_version_num=%d): open-discogs-model %s requires PostgreSQL %d or newer; upgrade PostgreSQL before starting the import",
		version.major(),
		version,
		canonicalModelVersion,
		minimumPostgreSQLServerVersion.major(),
	)
}

func (version postgreSQLServerVersionNumber) major() int {
	return int(version) / postgreSQLMajorVersionNumberScale
}
