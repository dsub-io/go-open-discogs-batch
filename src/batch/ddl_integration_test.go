package batch

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	opendiscogsschema "github.com/dsub-io/open-discogs-model/schema"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const legacyCustomChecksumFixture = "9:00000000000000000000000000000000"

func openMigrationPostgreSQL(t *testing.T) *gorm.DB {
	t.Helper()
	postgres := testutils.GetDatabase(t, testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, postgres))
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sqlDB.Close())
	})
	return db
}

func ensureMigrationTestSchema(t *testing.T, db *gorm.DB, name string) database.Schema {
	t.Helper()
	schema := requireMigrationSchema(t, name)
	_, err := database.EnsureSchema(db, schema)
	require.NoError(t, err)
	return schema
}

func requireCanonicalMigrationLedger(t *testing.T, db *gorm.DB, schema database.Schema) {
	t.Helper()
	migrationFS, err := loadSchemaMigrations()
	require.NoError(t, err)
	expected, err := readCanonicalMigrations(migrationFS, schema)
	require.NoError(t, err)

	var actual []struct {
		Version  string
		Checksum string
	}
	require.NoError(t, db.Raw(
		"select version, checksum from "+schema.Qualify(migrationTable)+" order by version",
	).Scan(&actual).Error)
	require.Len(t, actual, len(expected))
	for index, migration := range expected {
		require.Equal(t, migration.version, actual[index].Version)
		require.Equal(t, migration.checksum, actual[index].Checksum)
	}
}

func requirePostgreSQLRelation(
	t *testing.T,
	db *gorm.DB,
	schema database.Schema,
	relation string,
) {
	t.Helper()
	var exists bool
	require.NoError(t, db.Raw(
		"select to_regclass(?) is not null",
		schema.Name()+"."+relation,
	).Scan(&exists).Error)
	require.True(t, exists, "%s.%s must exist", schema.Name(), relation)
}

func requireBoundedPostgreSQLMigration(
	t *testing.T,
	db *gorm.DB,
	schema database.Schema,
) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		result <- RunDDLInSchema(db, schema.Name())
	}()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
		t.Fatal("schema migration did not complete with a single pooled connection")
	}
}

func requireTrigramExtensionSchema(t *testing.T, db *gorm.DB, expected string) {
	t.Helper()
	var actual migrationExtensionSchema
	require.NoError(t, db.Raw(trigramExtensionSchemaSQL).Scan(&actual).Error)
	require.Equal(t, expected, actual.Name)
}

func seedCustomLegacyLiquibasePrefix(
	t *testing.T,
	db *gorm.DB,
	schema database.Schema,
	prefixLength int,
) {
	t.Helper()
	migrationFS, err := loadSchemaMigrations()
	require.NoError(t, err)
	migrations, err := readCanonicalMigrations(migrationFS, schema)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(migrations), prefixLength)
	for _, migration := range migrations[:prefixLength] {
		require.NoError(t, db.Exec(migration.sql).Error)
	}
	require.NoError(t, db.Exec(`
		create table `+schema.Qualify(liquibaseChangeLogTable)+` (
			id varchar(255) not null,
			author varchar(255) not null,
			filename varchar(255) not null,
			exectype varchar(10) not null,
			md5sum varchar(35)
		)`).Error)
	manifest, err := opendiscogsschema.LegacyLiquibaseCompatibility()
	require.NoError(t, err)
	for _, migration := range manifest.Migrations[:prefixLength] {
		var changeSet *opendiscogsschema.LegacyChangeSet
		for index := range migration.LegacyChangeSets {
			candidate := &migration.LegacyChangeSets[index]
			if candidate.SchemaMode == opendiscogsschema.LegacySchemaModeCustom {
				changeSet = candidate
				break
			}
		}
		require.NotNil(t, changeSet)
		require.NoError(t, db.Exec(
			"insert into "+schema.Qualify(liquibaseChangeLogTable)+
				" (id, author, filename, exectype, md5sum) values (?, ?, ?, ?, ?)",
			changeSet.ID,
			changeSet.Author,
			changeSet.Filename,
			string(opendiscogsschema.LegacyExecutionTypeExecuted),
			legacyCustomChecksumFixture,
		).Error)
	}
}

func TestPostgreSQLMigrationContracts(t *testing.T) {
	db := openMigrationPostgreSQL(t)

	t.Run("keeps shared extension after first custom schema is dropped", func(t *testing.T) {
		first := ensureMigrationTestSchema(t, db, "migration_first")
		second := ensureMigrationTestSchema(t, db, "migration_second")
		sqlDB, err := db.DB()
		require.NoError(t, err)
		sqlDB.SetMaxOpenConns(1)

		requireBoundedPostgreSQLMigration(t, db, first)
		requireBoundedPostgreSQLMigration(t, db, second)
		sqlDB.SetMaxOpenConns(4)

		requireCanonicalMigrationLedger(t, db, first)
		requireCanonicalMigrationLedger(t, db, second)
		for _, schema := range []database.Schema{first, second} {
			requirePostgreSQLRelation(t, db, schema, "artist")
			requirePostgreSQLRelation(t, db, schema, "ix_artist_name_trgm")
		}

		requireTrigramExtensionSchema(t, db, database.DefaultSchemaName)
		require.NoError(t, db.Exec("drop schema "+first.Identifier()+" cascade").Error)
		requireTrigramExtensionSchema(t, db, database.DefaultSchemaName)
		requirePostgreSQLRelation(t, db, second, "ix_artist_name_trgm")
		requirePostgreSQLRelation(t, db, second, "ix_release_item_title_trgm")
	})

	t.Run("serializes the same new migration", func(t *testing.T) {
		schema := ensureMigrationTestSchema(t, db, "migration_concurrent")
		require.NoError(t, ensureMigrationLedger(db, schema))
		const (
			version = "V900__concurrent_fixture.sql"
			sqlText = `create table "migration_concurrent".fixture(id integer)`
		)
		checksum := migrationChecksum(sqlText)
		start := make(chan struct{})
		errors := make(chan error, 2)
		var waitGroup sync.WaitGroup
		for range 2 {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				<-start
				errors <- applyMigration(db, schema, version, checksum, sqlText)
			}()
		}
		close(start)
		waitGroup.Wait()
		close(errors)
		for err := range errors {
			require.NoError(t, err)
		}

		requirePostgreSQLRelation(t, db, schema, "fixture")
		var ledgerRows int64
		require.NoError(t, db.Raw(
			"select count(*) from "+schema.Qualify(migrationTable)+" where version = ?",
			version,
		).Scan(&ledgerRows).Error)
		require.EqualValues(t, 1, ledgerRows)
	})

	t.Run("rejects migration while an import lock is active", func(t *testing.T) {
		active, err := acquireMigrationImportLocks(db)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, active.release())
		})

		blocked, err := acquireMigrationImportLocks(db)
		require.Nil(t, blocked)
		require.ErrorContains(t, err, "artist import is active")

		require.NoError(t, active.release())
		reacquired, err := acquireMigrationImportLocks(db)
		require.NoError(t, err)
		require.NoError(t, reacquired.release())
	})

	t.Run("adopts a verified custom Liquibase V007 prefix", func(t *testing.T) {
		schema := ensureMigrationTestSchema(t, db, "migration_legacy_v007")
		seedCustomLegacyLiquibasePrefix(t, db, schema, 7)

		require.NoError(t, RunDDLInSchema(db, schema.Name()))
		requireCanonicalMigrationLedger(t, db, schema)

		var locked bool
		require.NoError(t, db.Raw(
			"select locked from "+schema.Qualify(liquibaseLockTable)+" where id = ?",
			liquibaseLockID,
		).Scan(&locked).Error)
		require.False(t, locked)

		var revisionColumn bool
		require.NoError(t, db.Raw(
			`select exists (
				select 1 from information_schema.columns
				where table_schema = ?
				  and table_name = 'discogs_import_run_dump'
				  and column_name = 'import_contract_revision'
			)`,
			schema.Name(),
		).Scan(&revisionColumn).Error)
		require.True(t, revisionColumn)

		var adoptedChecksums []string
		require.NoError(t, db.Raw(
			"select checksum from "+schema.Qualify(migrationTable)+
				" order by version limit ?",
			7,
		).Scan(&adoptedChecksums).Error)
		require.Len(t, adoptedChecksums, 7)
		for _, checksum := range adoptedChecksums {
			require.Len(t, strings.TrimSpace(checksum), 64)
		}
	})
}
