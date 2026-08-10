package batch

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"

	opendiscogsschema "github.com/dsub-io/open-discogs-model/schema"
	"gorm.io/gorm"
)

const migrationTable = "open_discogs_schema_migration"

var loadSchemaMigrations = opendiscogsschema.Migrations

// RunDDL applies the versioned PostgreSQL migrations embedded in open-discogs-model.
// Applied checksums are persisted so a released migration can never change silently.
func RunDDL(db *gorm.DB) error {
	migrations, err := loadSchemaMigrations()
	if err != nil {
		return fmt.Errorf("load shared schema migrations: %w", err)
	}
	return runDDL(db, migrations)
}

func runDDL(db *gorm.DB, migrations fs.FS) error {
	names, _ := fs.Glob(migrations, "*.sql")
	sort.Strings(names)

	if err := db.Exec(`
		create table if not exists public.open_discogs_schema_migration
		(
		    version     varchar(255) primary key,
		    checksum    char(64) not null,
		    applied_at  timestamp not null default now()
		)`).Error; err != nil {
		return fmt.Errorf("create schema migration ledger: %w", err)
	}

	for _, name := range names {
		contents, readErr := fs.ReadFile(migrations, name)
		if readErr != nil {
			return fmt.Errorf("read shared migration %s: %w", name, readErr)
		}
		sum := sha256.Sum256(contents)
		checksum := hex.EncodeToString(sum[:])

		if err := applyMigration(db, filepath.Base(name), checksum, string(contents)); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(db *gorm.DB, version, checksum, sql string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var appliedChecksum string
		result := tx.Raw(
			"select checksum from public.open_discogs_schema_migration where version = ?",
			version,
		).Scan(&appliedChecksum)
		if result.Error != nil {
			return fmt.Errorf("read migration ledger for %s: %w", version, result.Error)
		}
		if result.RowsAffected > 0 {
			if appliedChecksum != checksum {
				return fmt.Errorf(
					"shared migration %s checksum changed: database=%s artifact=%s",
					version,
					appliedChecksum,
					checksum,
				)
			}
			return nil
		}

		if err := tx.Exec(sql).Error; err != nil {
			return fmt.Errorf("apply shared migration %s: %w", version, err)
		}
		if err := tx.Exec(
			"insert into public.open_discogs_schema_migration (version, checksum) values (?, ?)",
			version,
			checksum,
		).Error; err != nil {
			return fmt.Errorf("record shared migration %s: %w", version, err)
		}
		return nil
	})
}
