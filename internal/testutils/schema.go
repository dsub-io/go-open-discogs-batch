package testutils

import (
	"fmt"
	"io/fs"
	"sort"

	opendiscogsschema "github.com/dsub-io/open-discogs-model/schema"
	"gorm.io/gorm"
)

func ApplySharedSchema(db *gorm.DB) error {
	migrations, err := opendiscogsschema.Migrations()
	if err != nil {
		return err
	}
	names, err := fs.Glob(migrations, "*.sql")
	if err != nil {
		return err
	}
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
