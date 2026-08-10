package testutils

import (
	"fmt"
	"io/fs"
	"sort"

	opendiscogsschema "github.com/dsub-io/open-discogs-model/schema"
	"gorm.io/gorm"
)

var loadSharedMigrations = opendiscogsschema.Migrations

func ApplySharedSchema(db *gorm.DB) error {
	migrations, err := loadSharedMigrations()
	if err != nil {
		return err
	}
	return applySharedMigrations(db, migrations)
}

func applySharedMigrations(db *gorm.DB, migrations fs.FS) error {
	names, _ := fs.Glob(migrations, "*.sql")
	sort.Strings(names)
	for _, name := range names {
		contents, readErr := fs.ReadFile(migrations, name)
		if readErr != nil {
			return readErr
		}
		if err := db.Exec(string(contents)).Error; err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	return nil
}
