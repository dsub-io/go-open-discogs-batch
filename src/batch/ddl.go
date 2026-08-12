package batch

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dsub-io/go-open-discogs-batch/src/database"
	opendiscogsmanifest "github.com/dsub-io/open-discogs-model/manifest"
	opendiscogsschema "github.com/dsub-io/open-discogs-model/schema"
	"gorm.io/gorm"
)

const (
	migrationTable             = "open_discogs_schema_migration"
	migrationBootstrapLockName = "open-discogs-schema-migration"
	migrationImportLockSQL     = "select pg_try_advisory_lock($1, $2)"
	migrationImportUnlockSQL   = "select pg_advisory_unlock($1, $2)"
	trigramExtensionSchemaSQL  = "select namespace.nspname as name, quote_ident(namespace.nspname) as identifier from pg_extension extension join pg_namespace namespace on namespace.oid = extension.extnamespace where extension.extname = 'pg_trgm'"
	migrationBootstrapLockSQL  = "select pg_try_advisory_xact_lock(hashtextextended(current_database() || ':' || ?, 0))"
)

var loadSchemaMigrations = opendiscogsschema.Migrations
var requiredMigrationLockTypes = opendiscogsmanifest.RequiredLockEntityTypes
var migrationEntityLockKey = opendiscogsmanifest.EntityLockKey

type canonicalMigration struct {
	version  string
	checksum string
	sql      string
}

type migrationImportLocks struct {
	connection *sql.Conn
	db         *gorm.DB
	keys       []int32
}

type migrationExtensionSchema struct {
	Name       string
	Identifier string
}

// RunDDL applies the versioned PostgreSQL migrations embedded in open-discogs-model.
// Applied checksums are persisted so a released migration can never change silently.
func RunDDL(db *gorm.DB) error {
	return RunDDLInSchema(db, database.DefaultSchemaName)
}

// RunDDLInSchema applies canonical migrations inside the selected PostgreSQL schema.
func RunDDLInSchema(db *gorm.DB, schemaName string) error {
	migrations, err := loadSchemaMigrations()
	if err != nil {
		return fmt.Errorf("load shared schema migrations: %w", err)
	}
	schema, err := database.ParseSchema(schemaName)
	if err != nil {
		return err
	}
	return runDDL(db, migrations, schema)
}

func runDDL(db *gorm.DB, migrations fs.FS, schema database.Schema) (runErr error) {
	loaded, err := readCanonicalMigrations(migrations, schema)
	if err != nil {
		return err
	}
	locks, err := acquireMigrationImportLocks(db)
	if err != nil {
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, locks.release())
	}()

	migrationDB := locks.db
	liquibaseLock, err := acquireLiquibaseMigrationLock(migrationDB, schema)
	if err != nil {
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, liquibaseLock.release())
	}()
	if err := ensureMigrationLedger(migrationDB, schema); err != nil {
		return fmt.Errorf("create schema migration ledger: %w", err)
	}
	if err := adoptLegacyLiquibaseHistory(migrationDB, schema, loaded); err != nil {
		return err
	}
	if err := validateMigrationLedger(migrationDB, schema, loaded); err != nil {
		return err
	}

	for _, migration := range loaded {
		if err := applyMigration(
			migrationDB,
			schema,
			migration.version,
			migration.checksum,
			migration.sql,
		); err != nil {
			return err
		}
	}
	return nil
}

func readCanonicalMigrations(
	migrations fs.FS,
	schema database.Schema,
) ([]canonicalMigration, error) {
	names, err := fs.Glob(migrations, "*.sql")
	if err != nil {
		return nil, fmt.Errorf("list shared schema migrations: %w", err)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("shared schema migration inventory is empty")
	}
	loaded := make([]canonicalMigration, 0, len(names))
	for _, name := range names {
		contents, readErr := fs.ReadFile(migrations, name)
		if readErr != nil {
			return nil, fmt.Errorf("read shared migration %s: %w", name, readErr)
		}
		sum := sha256.Sum256(contents)
		loaded = append(loaded, canonicalMigration{
			version:  filepath.Base(name),
			checksum: hex.EncodeToString(sum[:]),
			sql:      schema.ScopeCanonicalSQL(string(contents)),
		})
	}
	return loaded, nil
}

func acquireMigrationImportLocks(db *gorm.DB) (*migrationImportLocks, error) {
	if db == nil {
		return nil, fmt.Errorf("acquire schema migration import locks: database connection is nil")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("resolve schema migration database: %w", err)
	}
	connection, err := sqlDB.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("reserve schema migration lock connection: %w", err)
	}
	// Context forces GORM to clone Statement. Without that clone, replacing ConnPool here also
	// mutates the caller and leaves the importer bound to this connection after it is returned.
	migrationDB := db.Session(&gorm.Session{NewDB: true, Context: context.Background()})
	migrationDB.ConnPool = connection
	migrationDB.Statement.ConnPool = connection
	locks := &migrationImportLocks{connection: connection, db: migrationDB}
	lockTypes, err := requiredMigrationLockTypes([]string{"release"})
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("resolve schema migration import locks: %w", err)
	}
	for _, entityType := range lockTypes {
		key, keyErr := migrationEntityLockKey(entityType)
		if keyErr != nil {
			_ = locks.release()
			return nil, fmt.Errorf("resolve %s schema migration lock: %w", entityType, keyErr)
		}
		var acquired bool
		queryErr := connection.QueryRowContext(
			context.Background(),
			migrationImportLockSQL,
			opendiscogsmanifest.AdvisoryLockNamespace,
			key,
		).Scan(&acquired)
		if queryErr != nil {
			_ = locks.release()
			return nil, fmt.Errorf("acquire %s schema migration lock: %w", entityType, queryErr)
		}
		if !acquired {
			_ = locks.release()
			return nil, fmt.Errorf(
				"cannot migrate schema while an %s import is active; stop every Go and Java importer and retry",
				entityType,
			)
		}
		locks.keys = append(locks.keys, key)
	}
	return locks, nil
}

func (locks *migrationImportLocks) release() error {
	if locks == nil || locks.connection == nil {
		return nil
	}
	var releaseErrors []error
	for index := len(locks.keys) - 1; index >= 0; index-- {
		var released bool
		if err := locks.connection.QueryRowContext(
			context.Background(),
			migrationImportUnlockSQL,
			opendiscogsmanifest.AdvisoryLockNamespace,
			locks.keys[index],
		).Scan(&released); err != nil {
			releaseErrors = append(releaseErrors, fmt.Errorf("release schema migration import lock: %w", err))
		} else if !released {
			releaseErrors = append(
				releaseErrors,
				fmt.Errorf("schema migration advisory lock was not held: %d", locks.keys[index]),
			)
		}
	}
	locks.keys = nil
	if len(releaseErrors) > 0 {
		locks.discardConnection()
	}
	if err := locks.connection.Close(); err != nil {
		releaseErrors = append(releaseErrors, fmt.Errorf("close schema migration lock connection: %w", err))
	}
	locks.connection = nil
	locks.db = nil
	return errors.Join(releaseErrors...)
}

func (locks *migrationImportLocks) discardConnection() {
	if locks == nil || locks.connection == nil {
		return
	}
	_ = locks.connection.Raw(func(any) error { return driver.ErrBadConn })
}

func validateMigrationLedger(
	db *gorm.DB,
	schema database.Schema,
	migrations []canonicalMigration,
) error {
	var applied []struct {
		Version  string
		Checksum string
	}
	if err := db.Raw(
		"select version, checksum from " + schema.Qualify(migrationTable) + " order by version",
	).Scan(&applied).Error; err != nil {
		return fmt.Errorf("read shared migration inventory: %w", err)
	}
	if len(applied) > len(migrations) {
		return fmt.Errorf("database schema contains migrations newer than this batch artifact")
	}
	for index, row := range applied {
		expected := migrations[index]
		if row.Version != expected.version {
			return fmt.Errorf(
				"database migration history is not a canonical prefix: position=%d database=%s artifact=%s",
				index,
				row.Version,
				expected.version,
			)
		}
		if row.Checksum != expected.checksum {
			return fmt.Errorf(
				"shared migration %s checksum changed: database=%s artifact=%s",
				row.Version,
				row.Checksum,
				expected.checksum,
			)
		}
	}
	return nil
}

func ensureMigrationLedger(db *gorm.DB, schema database.Schema) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var acquired bool
		if err := tx.Raw(
			migrationBootstrapLockSQL,
			migrationBootstrapLockName+":"+schema.Name(),
		).Scan(&acquired).Error; err != nil {
			return fmt.Errorf("lock schema migration bootstrap: %w", err)
		}
		if !acquired {
			return fmt.Errorf("another schema migrator is active for schema %s", schema.Name())
		}
		return tx.Exec(fmt.Sprintf(`
			create table if not exists %s
			(
			    version     varchar(255) primary key,
			    checksum    char(64) not null,
			    applied_at  timestamp not null default now()
			)`, schema.Qualify(migrationTable))).Error
	})
}

func applyMigration(db *gorm.DB, schema database.Schema, version, checksum, sql string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"lock table " + schema.Qualify(migrationTable) + " in exclusive mode",
		).Error; err != nil {
			return fmt.Errorf("lock shared migration ledger: %w", err)
		}
		var appliedChecksum string
		result := tx.Raw(
			"select checksum from "+schema.Qualify(migrationTable)+" where version = ?",
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

		if err := setMigrationSearchPath(tx, schema); err != nil {
			return err
		}
		if err := tx.Exec(sql).Error; err != nil {
			return fmt.Errorf("apply shared migration %s: %w", version, err)
		}
		if err := tx.Exec(
			"insert into "+schema.Qualify(migrationTable)+" (version, checksum) values (?, ?)",
			version,
			checksum,
		).Error; err != nil {
			return fmt.Errorf("record shared migration %s: %w", version, err)
		}
		return nil
	})
}

func setMigrationSearchPath(tx *gorm.DB, schema database.Schema) error {
	pathNames := make(map[string]struct{}, 3)
	path := make([]string, 0, 3)
	appendPath := func(name, identifier string) {
		if _, exists := pathNames[name]; exists {
			return
		}
		pathNames[name] = struct{}{}
		path = append(path, identifier)
	}
	var extensionSchema migrationExtensionSchema
	result := tx.Raw(trigramExtensionSchemaSQL).Scan(&extensionSchema)
	if result.Error != nil {
		return fmt.Errorf("inspect pg_trgm extension schema: %w", result.Error)
	}
	if result.RowsAffected > 0 {
		appendPath(extensionSchema.Name, extensionSchema.Identifier)
	} else {
		appendPath(database.DefaultSchemaName, `"`+database.DefaultSchemaName+`"`)
	}
	appendPath(schema.Name(), schema.Identifier())
	appendPath(database.DefaultSchemaName, `"`+database.DefaultSchemaName+`"`)
	if err := tx.Exec("set local search_path to " + strings.Join(path, ", ")).Error; err != nil {
		return fmt.Errorf("set schema migration search path: %w", err)
	}
	return nil
}
