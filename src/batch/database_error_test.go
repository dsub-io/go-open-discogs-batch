package batch

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io/fs"
	"regexp"
	"testing"
	"testing/fstest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	opendiscogsmanifest "github.com/dsub-io/open-discogs-model/manifest"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type postgresArrayValueConverter struct{}

func (postgresArrayValueConverter) ConvertValue(value interface{}) (driver.Value, error) {
	switch value.(type) {
	case pgtype.Array[int32], pgtype.Array[string], pgtype.Array[[]byte]:
		return "array", nil
	default:
		return driver.DefaultParameterConverter.ConvertValue(value)
	}
}

func newMockGorm(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.ValueConverterOption(postgresArrayValueConverter{}),
	)
	require.NoError(t, err)
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{
		DisableAutomaticPing:   true,
		SkipDefaultTransaction: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		for range sqlDB.Stats().OpenConnections {
			mock.ExpectClose()
		}
		require.NoError(t, sqlDB.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
	return db, mock, sqlDB
}

type migrationGlobFailureFS struct {
	err error
}

func (failure migrationGlobFailureFS) Open(string) (fs.File, error) {
	return nil, fs.ErrNotExist
}

func (failure migrationGlobFailureFS) Glob(string) ([]string, error) {
	return nil, failure.err
}

type unsupportedMigrationConnPool struct{}

func (unsupportedMigrationConnPool) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, errors.New("unsupported")
}

func (unsupportedMigrationConnPool) ExecContext(
	context.Context,
	string,
	...interface{},
) (sql.Result, error) {
	return nil, errors.New("unsupported")
}

func (unsupportedMigrationConnPool) QueryContext(
	context.Context,
	string,
	...interface{},
) (*sql.Rows, error) {
	return nil, errors.New("unsupported")
}

func (unsupportedMigrationConnPool) QueryRowContext(
	context.Context,
	string,
	...interface{},
) *sql.Row {
	return new(sql.Row)
}

func requireMigrationSchema(t *testing.T, name string) database.Schema {
	t.Helper()
	schema, err := database.ParseSchema(name)
	require.NoError(t, err)
	return schema
}

func migrationFixture(contents string) fstest.MapFS {
	return fstest.MapFS{
		"001.sql": &fstest.MapFile{Data: []byte(contents)},
	}
}

func migrationChecksum(contents string) string {
	sum := sha256.Sum256([]byte(contents))
	return hex.EncodeToString(sum[:])
}

func requiredMigrationLockKeys(t *testing.T) []int32 {
	t.Helper()
	entityTypes, err := opendiscogsmanifest.RequiredLockEntityTypes([]string{"release"})
	require.NoError(t, err)
	keys := make([]int32, 0, len(entityTypes))
	for _, entityType := range entityTypes {
		key, keyErr := opendiscogsmanifest.EntityLockKey(entityType)
		require.NoError(t, keyErr)
		keys = append(keys, key)
	}
	return keys
}

func expectMigrationLock(mock sqlmock.Sqlmock, key int32, acquired bool) {
	mock.ExpectQuery(regexp.QuoteMeta(migrationImportLockSQL)).
		WithArgs(opendiscogsmanifest.AdvisoryLockNamespace, key).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(acquired))
}

func expectAllMigrationLocks(t *testing.T, mock sqlmock.Sqlmock) []int32 {
	t.Helper()
	keys := requiredMigrationLockKeys(t)
	for _, key := range keys {
		expectMigrationLock(mock, key, true)
	}
	return keys
}

func expectMigrationUnlock(mock sqlmock.Sqlmock, key int32, released bool) {
	mock.ExpectQuery(regexp.QuoteMeta(migrationImportUnlockSQL)).
		WithArgs(opendiscogsmanifest.AdvisoryLockNamespace, key).
		WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(released))
}

func expectAllMigrationUnlocks(mock sqlmock.Sqlmock, keys []int32) {
	for index := len(keys) - 1; index >= 0; index-- {
		expectMigrationUnlock(mock, keys[index], true)
	}
}

func expectLiquibaseMigrationLock(mock sqlmock.Sqlmock, schema database.Schema) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		"create table if not exists " + schema.Qualify(liquibaseLockTable),
	)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(
		"insert into " + schema.Qualify(liquibaseLockTable) +
			" (id, locked) values ($1, false) on conflict (id) do nothing",
	)).WithArgs(liquibaseLockID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(
		"select locked from " + schema.Qualify(liquibaseLockTable) +
			" where id = $1 for update nowait",
	)).WithArgs(liquibaseLockID).
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(false))
	mock.ExpectExec(regexp.QuoteMeta(
		"update "+schema.Qualify(liquibaseLockTable)+
			" set locked = true, lockgranted = now(), lockedby = $1 where id = $2 and not locked",
	)).WithArgs(liquibaseMigrationLockOwner, liquibaseLockID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func expectNoLegacyLiquibaseHistory(mock sqlmock.Sqlmock, schema database.Schema) {
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		"lock table " + schema.Qualify(migrationTable) + " in exclusive mode",
	)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(
		"select count(*) from " + schema.Qualify(migrationTable),
	)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(regexp.QuoteMeta(
		"select exists(select 1 from information_schema.tables where table_schema = $1 and table_name = $2)",
	)).WithArgs(schema.Name(), liquibaseChangeLogTable).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectCommit()
}

func expectLiquibaseMigrationUnlock(mock sqlmock.Sqlmock, schema database.Schema) {
	mock.ExpectExec(regexp.QuoteMeta(
		"update "+schema.Qualify(liquibaseLockTable)+
			" set locked = false, lockgranted = null, lockedby = null where id = $1 and locked and lockedby = $2",
	)).WithArgs(liquibaseLockID, liquibaseMigrationLockOwner).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectMigrationLedger(t *testing.T, mock sqlmock.Sqlmock, schema database.Schema) {
	t.Helper()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`select pg_try_advisory_xact_lock(hashtextextended(current_database() || ':' || $1, 0))`,
	)).WithArgs(migrationBootstrapLockName + ":" + schema.Name()).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(true))
	mock.ExpectExec(regexp.QuoteMeta(
		"create table if not exists " + schema.Qualify(migrationTable),
	)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
}

func expectMigrationInventory(
	mock sqlmock.Sqlmock,
	schema database.Schema,
	rows *sqlmock.Rows,
) {
	mock.ExpectQuery(regexp.QuoteMeta(
		"select version, checksum from " + schema.Qualify(migrationTable) + " order by version",
	)).WillReturnRows(rows)
}

func expectTrigramExtensionSchema(mock sqlmock.Sqlmock, rows *sqlmock.Rows) {
	mock.ExpectQuery(regexp.QuoteMeta(trigramExtensionSchemaSQL)).
		WillReturnRows(rows)
}

func expectMigrationSearchPath(mock sqlmock.Sqlmock, searchPath string) {
	mock.ExpectExec(regexp.QuoteMeta("set local search_path to " + searchPath)).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func emptyTrigramExtensionRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"name", "identifier"})
}

func trigramExtensionRows(name, identifier string) *sqlmock.Rows {
	return emptyTrigramExtensionRows().AddRow(name, identifier)
}

func requireBoundedMigrationResult(t *testing.T, operation func() error) error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		result <- operation()
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("migration operation did not complete within two seconds")
		return nil
	}
}

func TestRunDDLErrorBoundaries(t *testing.T) {
	originalLoad := loadSchemaMigrations
	t.Cleanup(func() { loadSchemaMigrations = originalLoad })
	expected := errors.New("fixture")

	loadSchemaMigrations = func() (fs.FS, error) { return nil, expected }
	require.ErrorContains(t, RunDDL(nil), "load shared schema migrations")
	loadSchemaMigrations = originalLoad
	require.ErrorContains(t, RunDDLInSchema(nil, "Invalid"), "database-schema")

	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	db, mock, _ := newMockGorm(t)
	keys := expectAllMigrationLocks(t, mock)
	expectLiquibaseMigrationLock(mock, schema)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(
		`select pg_try_advisory_xact_lock(hashtextextended(current_database() || ':' || $1, 0))`,
	)).WithArgs(migrationBootstrapLockName + ":" + schema.Name()).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(true))
	mock.ExpectExec(regexp.QuoteMeta(
		"create table if not exists " + schema.Qualify(migrationTable),
	)).
		WillReturnError(expected)
	mock.ExpectRollback()
	expectLiquibaseMigrationUnlock(mock, schema)
	expectAllMigrationUnlocks(mock, keys)
	require.ErrorContains(
		t,
		runDDL(db, migrationFixture("select 1"), schema),
		"create schema migration ledger",
	)
}

func TestReadCanonicalMigrationsFailBeforeDatabaseAccess(t *testing.T) {
	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	expected := errors.New("fixture")

	require.ErrorContains(
		t,
		runDDL(nil, migrationGlobFailureFS{err: expected}, schema),
		"list shared schema migrations",
	)
	require.ErrorContains(
		t,
		runDDL(nil, fstest.MapFS{}, schema),
		"migration inventory is empty",
	)
	require.ErrorContains(t, runDDL(nil, fstest.MapFS{
		"001-broken.sql": &fstest.MapFile{Mode: fs.ModeDir},
	}, schema), "read shared migration")
}

func TestReadCanonicalMigrationsSortsChecksumsAndScopesSchema(t *testing.T) {
	schema := requireMigrationSchema(t, "open_discogs")
	firstSQL := "create table public.first(id integer)"
	secondSQL := "create table public.second(id integer)"
	migrations, err := readCanonicalMigrations(fstest.MapFS{
		"V002__second.sql": &fstest.MapFile{Data: []byte(secondSQL)},
		"V001__first.sql":  &fstest.MapFile{Data: []byte(firstSQL)},
	}, schema)
	require.NoError(t, err)
	require.Equal(t, []canonicalMigration{
		{
			version:  "V001__first.sql",
			checksum: migrationChecksum(firstSQL),
			sql:      `create table "open_discogs".first(id integer)`,
		},
		{
			version:  "V002__second.sql",
			checksum: migrationChecksum(secondSQL),
			sql:      `create table "open_discogs".second(id integer)`,
		},
	}, migrations)
}

func TestRunDDLUsesReservedConnectionWithSingleConnectionPool(t *testing.T) {
	const contents = "create table public.fixture(id integer)"
	schema := requireMigrationSchema(t, "open_discogs")
	db, mock, sqlDB := newMockGorm(t)
	sqlDB.SetMaxOpenConns(1)
	keys := expectAllMigrationLocks(t, mock)
	expectLiquibaseMigrationLock(mock, schema)
	expectMigrationLedger(t, mock, schema)
	expectNoLegacyLiquibaseHistory(mock, schema)
	expectMigrationInventory(
		mock,
		schema,
		sqlmock.NewRows([]string{"version", "checksum"}),
	)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		"lock table " + schema.Qualify(migrationTable) + " in exclusive mode",
	)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(
		"select checksum from " + schema.Qualify(migrationTable) + " where version = $1",
	)).WithArgs("001.sql").
		WillReturnRows(sqlmock.NewRows([]string{"checksum"}))
	expectTrigramExtensionSchema(mock, emptyTrigramExtensionRows())
	expectMigrationSearchPath(mock, `"public", "open_discogs"`)
	mock.ExpectExec(regexp.QuoteMeta(
		`create table "open_discogs".fixture(id integer)`,
	)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(
		"insert into "+schema.Qualify(migrationTable)+" (version, checksum) values ($1, $2)",
	)).WithArgs("001.sql", migrationChecksum(contents)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectLiquibaseMigrationUnlock(mock, schema)
	expectAllMigrationUnlocks(mock, keys)

	require.NoError(t, requireBoundedMigrationResult(
		t,
		func() error { return runDDL(db, migrationFixture(contents), schema) },
	))
}

func TestRunDDLPropagatesMigrationAndReleaseFailures(t *testing.T) {
	const contents = "select 1"
	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	db, mock, _ := newMockGorm(t)
	expectedMigration := errors.New("migration fixture")
	expectedRelease := errors.New("release fixture")
	keys := expectAllMigrationLocks(t, mock)
	expectLiquibaseMigrationLock(mock, schema)
	expectMigrationLedger(t, mock, schema)
	expectNoLegacyLiquibaseHistory(mock, schema)
	expectMigrationInventory(
		mock,
		schema,
		sqlmock.NewRows([]string{"version", "checksum"}),
	)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		"lock table " + schema.Qualify(migrationTable) + " in exclusive mode",
	)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(
		"select checksum from " + schema.Qualify(migrationTable) + " where version = $1",
	)).WithArgs("001.sql").WillReturnError(expectedMigration)
	mock.ExpectRollback()
	expectLiquibaseMigrationUnlock(mock, schema)
	for index := len(keys) - 1; index >= 0; index-- {
		expectation := mock.ExpectQuery(regexp.QuoteMeta(migrationImportUnlockSQL)).
			WithArgs(opendiscogsmanifest.AdvisoryLockNamespace, keys[index])
		if index == len(keys)-1 {
			expectation.WillReturnError(expectedRelease)
		} else {
			expectation.WillReturnRows(sqlmock.NewRows([]string{"released"}).AddRow(true))
		}
	}

	err := runDDL(db, migrationFixture(contents), schema)
	require.ErrorIs(t, err, expectedMigration)
	require.ErrorIs(t, err, expectedRelease)
}

func TestRunDDLRejectsActiveImportAndInvalidLedger(t *testing.T) {
	const contents = "select 1"
	schema := requireMigrationSchema(t, database.DefaultSchemaName)

	t.Run("active import", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		keys := requiredMigrationLockKeys(t)
		expectMigrationLock(mock, keys[0], false)

		err := runDDL(db, migrationFixture(contents), schema)
		require.ErrorContains(t, err, "artist import is active")
	})

	t.Run("database ledger ahead", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		keys := expectAllMigrationLocks(t, mock)
		expectLiquibaseMigrationLock(mock, schema)
		expectMigrationLedger(t, mock, schema)
		expectNoLegacyLiquibaseHistory(mock, schema)
		expectMigrationInventory(
			mock,
			schema,
			sqlmock.NewRows([]string{"version", "checksum"}).
				AddRow("001.sql", migrationChecksum(contents)).
				AddRow("002.sql", "newer"),
		)
		expectLiquibaseMigrationUnlock(mock, schema)
		expectAllMigrationUnlocks(mock, keys)

		err := runDDL(db, migrationFixture(contents), schema)
		require.ErrorContains(t, err, "newer than this batch artifact")
	})
}

func TestEnsureMigrationLedgerTryLockBoundaries(t *testing.T) {
	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	expected := errors.New("fixture")

	for _, test := range []struct {
		name       string
		rows       *sqlmock.Rows
		queryError error
		create     bool
		want       string
	}{
		{
			name:   "acquired",
			rows:   sqlmock.NewRows([]string{"acquired"}).AddRow(true),
			create: true,
		},
		{
			name: "not acquired",
			rows: sqlmock.NewRows([]string{"acquired"}).AddRow(false),
			want: "another schema migrator is active",
		},
		{
			name: "no row",
			rows: sqlmock.NewRows([]string{"acquired"}),
			want: "another schema migrator is active",
		},
		{
			name:       "query error",
			queryError: expected,
			want:       "lock schema migration bootstrap",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, _ := newMockGorm(t)
			mock.ExpectBegin()
			query := mock.ExpectQuery(regexp.QuoteMeta(
				`select pg_try_advisory_xact_lock(hashtextextended(current_database() || ':' || $1, 0))`,
			)).
				WithArgs(migrationBootstrapLockName + ":" + schema.Name())
			if test.queryError != nil {
				query.WillReturnError(test.queryError)
			} else {
				query.WillReturnRows(test.rows)
			}
			if test.create {
				mock.ExpectExec(regexp.QuoteMeta(
					"create table if not exists " + schema.Qualify(migrationTable),
				)).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}

			err := requireBoundedMigrationResult(t, func() error {
				return ensureMigrationLedger(db, schema)
			})
			if test.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.want)
			}
		})
	}
}

func TestMigrationImportLockErrorBoundaries(t *testing.T) {
	expected := errors.New("fixture")
	require.ErrorContains(t, func() error {
		_, err := acquireMigrationImportLocks(nil)
		return err
	}(), "database connection is nil")

	t.Run("resolve database", func(t *testing.T) {
		db, _, _ := newMockGorm(t)
		pool := unsupportedMigrationConnPool{}
		db.ConnPool = pool
		db.Statement.ConnPool = pool
		_, err := acquireMigrationImportLocks(db)
		require.ErrorContains(t, err, "resolve schema migration database")
	})

	t.Run("required lock type resolution", func(t *testing.T) {
		original := requiredMigrationLockTypes
		requiredMigrationLockTypes = func([]string) ([]string, error) {
			return nil, expected
		}
		t.Cleanup(func() { requiredMigrationLockTypes = original })

		db, _, _ := newMockGorm(t)
		_, err := acquireMigrationImportLocks(db)
		require.ErrorIs(t, err, expected)
		require.ErrorContains(t, err, "resolve schema migration import locks")
	})

	t.Run("entity lock key resolution", func(t *testing.T) {
		originalTypes := requiredMigrationLockTypes
		originalKey := migrationEntityLockKey
		requiredMigrationLockTypes = func([]string) ([]string, error) {
			return []string{"artist", "invalid"}, nil
		}
		migrationEntityLockKey = func(entityType string) (int32, error) {
			if entityType == "artist" {
				return 101, nil
			}
			return 0, expected
		}
		t.Cleanup(func() {
			requiredMigrationLockTypes = originalTypes
			migrationEntityLockKey = originalKey
		})

		db, mock, _ := newMockGorm(t)
		expectMigrationLock(mock, 101, true)
		expectMigrationUnlock(mock, 101, true)
		_, err := acquireMigrationImportLocks(db)
		require.ErrorIs(t, err, expected)
		require.ErrorContains(t, err, "resolve invalid schema migration lock")
	})

	t.Run("reserve connection", func(t *testing.T) {
		sqlDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{
			DisableAutomaticPing:   true,
			SkipDefaultTransaction: true,
		})
		require.NoError(t, err)
		mock.ExpectClose()
		require.NoError(t, sqlDB.Close())
		_, err = acquireMigrationImportLocks(db)
		require.ErrorContains(t, err, "reserve schema migration lock connection")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	for _, test := range []struct {
		name       string
		lockResult *sqlmock.Rows
		lockError  error
		want       string
	}{
		{
			name:       "active import",
			lockResult: sqlmock.NewRows([]string{"acquired"}).AddRow(false),
			want:       "label import is active",
		},
		{
			name:      "lock query",
			lockError: expected,
			want:      "acquire label schema migration lock",
		},
		{
			name:       "lock query returned no row",
			lockResult: sqlmock.NewRows([]string{"acquired"}),
			want:       "acquire label schema migration lock",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, _ := newMockGorm(t)
			keys := requiredMigrationLockKeys(t)
			expectMigrationLock(mock, keys[0], true)
			expectation := mock.ExpectQuery(regexp.QuoteMeta(migrationImportLockSQL)).
				WithArgs(opendiscogsmanifest.AdvisoryLockNamespace, keys[1])
			if test.lockError != nil {
				expectation.WillReturnError(test.lockError)
			} else {
				expectation.WillReturnRows(test.lockResult)
			}
			expectMigrationUnlock(mock, keys[0], true)
			_, err := acquireMigrationImportLocks(db)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestMigrationImportLockReleaseBoundaries(t *testing.T) {
	var absent *migrationImportLocks
	require.NoError(t, absent.release())
	require.NoError(t, (&migrationImportLocks{}).release())
	absent.discardConnection()
	(&migrationImportLocks{}).discardConnection()

	t.Run("unlock success uses reverse order", func(t *testing.T) {
		_, mock, sqlDB := newMockGorm(t)
		connection, err := sqlDB.Conn(context.Background())
		require.NoError(t, err)
		keys := []int32{1, 2}
		expectMigrationUnlock(mock, keys[1], true)
		expectMigrationUnlock(mock, keys[0], true)
		locks := &migrationImportLocks{
			connection: connection,
			db:         &gorm.DB{},
			keys:       keys,
		}
		require.NoError(t, locks.release())
		require.Empty(t, locks.keys)
		require.Nil(t, locks.connection)
		require.Nil(t, locks.db)
		require.NoError(t, locks.release())
	})

	for _, test := range []struct {
		name       string
		rows       *sqlmock.Rows
		queryError error
		want       string
	}{
		{
			name: "unlock returned false",
			rows: sqlmock.NewRows([]string{"released"}).AddRow(false),
			want: "advisory lock was not held",
		},
		{
			name: "unlock returned no row",
			rows: sqlmock.NewRows([]string{"released"}),
			want: "sql: no rows in result set",
		},
		{
			name:       "unlock query failed",
			queryError: errors.New("unlock fixture"),
			want:       "unlock fixture",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, sqlDB := newMockGorm(t)
			sqlDB.SetMaxOpenConns(1)
			connection, err := sqlDB.Conn(context.Background())
			require.NoError(t, err)
			query := mock.ExpectQuery(regexp.QuoteMeta(migrationImportUnlockSQL)).
				WithArgs(opendiscogsmanifest.AdvisoryLockNamespace, int32(1))
			if test.queryError != nil {
				query.WillReturnError(test.queryError)
			} else {
				query.WillReturnRows(test.rows)
			}
			locks := &migrationImportLocks{connection: connection, db: db, keys: []int32{1}}
			err = locks.release()
			require.ErrorContains(t, err, test.want)
			require.Nil(t, locks.connection)
			require.Nil(t, locks.db)
			require.Zero(t, sqlDB.Stats().OpenConnections, "bad physical connection must be discarded")
		})
	}

	t.Run("connection already closed", func(t *testing.T) {
		_, _, sqlDB := newMockGorm(t)
		connection, err := sqlDB.Conn(context.Background())
		require.NoError(t, err)
		require.NoError(t, connection.Close())
		locks := &migrationImportLocks{connection: connection}
		require.ErrorIs(t, locks.release(), sql.ErrConnDone)
		require.Nil(t, locks.connection)
	})
}

func TestValidateMigrationLedger(t *testing.T) {
	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	migrations := []canonicalMigration{
		{version: "V001__first.sql", checksum: "first"},
		{version: "V002__second.sql", checksum: "second"},
	}

	for _, test := range []struct {
		name string
		rows *sqlmock.Rows
		want string
	}{
		{
			name: "empty prefix",
			rows: sqlmock.NewRows([]string{"version", "checksum"}),
		},
		{
			name: "exact prefix",
			rows: sqlmock.NewRows([]string{"version", "checksum"}).
				AddRow("V001__first.sql", "first"),
		},
		{
			name: "artifact behind database",
			rows: sqlmock.NewRows([]string{"version", "checksum"}).
				AddRow("V001__first.sql", "first").
				AddRow("V002__second.sql", "second").
				AddRow("V003__third.sql", "third"),
			want: "newer than this batch artifact",
		},
		{
			name: "history gap",
			rows: sqlmock.NewRows([]string{"version", "checksum"}).
				AddRow("V001__first.sql", "first").
				AddRow("V003__third.sql", "third"),
			want: "not a canonical prefix",
		},
		{
			name: "checksum mismatch",
			rows: sqlmock.NewRows([]string{"version", "checksum"}).
				AddRow("V001__first.sql", "changed"),
			want: "checksum changed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, _ := newMockGorm(t)
			expectMigrationInventory(mock, schema, test.rows)
			err := validateMigrationLedger(db, schema, migrations)
			if test.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.want)
			}
		})
	}

	t.Run("query failure", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		expected := errors.New("fixture")
		mock.ExpectQuery(regexp.QuoteMeta(
			"select version, checksum from " + schema.Qualify(migrationTable) + " order by version",
		)).WillReturnError(expected)
		require.ErrorContains(
			t,
			validateMigrationLedger(db, schema, migrations),
			"read shared migration inventory",
		)
	})
}

func TestApplyMigrationErrorBoundaries(t *testing.T) {
	expected := errors.New("fixture")
	const (
		version  = "001.sql"
		checksum = "new"
		sqlText  = "invalid SQL"
	)
	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  string
	}{
		{
			name: "ledger lock failure",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
					WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "lock shared migration ledger",
		},
		{
			name: "ledger query failure",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration" where version = $1`)).
					WithArgs(version).
					WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "read migration ledger",
		},
		{
			name: "changed checksum",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration" where version = $1`)).
					WithArgs(version).
					WillReturnRows(sqlmock.NewRows([]string{"checksum"}).AddRow("old"))
				mock.ExpectRollback()
			},
			want: "checksum changed",
		},
		{
			name: "migration execution failure",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration" where version = $1`)).
					WithArgs(version).
					WillReturnRows(sqlmock.NewRows([]string{"checksum"}))
				expectTrigramExtensionSchema(mock, emptyTrigramExtensionRows())
				expectMigrationSearchPath(mock, `"public"`)
				mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "apply shared migration",
		},
		{
			name: "search path inspection failure",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration" where version = $1`)).
					WithArgs(version).
					WillReturnRows(sqlmock.NewRows([]string{"checksum"}))
				mock.ExpectQuery(regexp.QuoteMeta(trigramExtensionSchemaSQL)).
					WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "inspect pg_trgm extension schema",
		},
		{
			name: "ledger insert failure",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration" where version = $1`)).
					WithArgs(version).
					WillReturnRows(sqlmock.NewRows([]string{"checksum"}))
				expectTrigramExtensionSchema(mock, emptyTrigramExtensionRows())
				expectMigrationSearchPath(mock, `"public"`)
				mock.ExpectExec(regexp.QuoteMeta(sqlText)).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta(`insert into "public"."open_discogs_schema_migration" (version, checksum) values ($1, $2)`)).
					WithArgs(version, checksum).
					WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "record shared migration",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := requireMigrationSchema(t, database.DefaultSchemaName)
			db, mock, _ := newMockGorm(t)
			test.setup(mock)
			require.ErrorContains(t, applyMigration(db, schema, version, checksum, sqlText), test.want)
		})
	}

	t.Run("already applied checksum", func(t *testing.T) {
		schema := requireMigrationSchema(t, database.DefaultSchemaName)
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration" where version = $1`)).
			WithArgs(version).
			WillReturnRows(sqlmock.NewRows([]string{"checksum"}).AddRow(checksum))
		mock.ExpectCommit()
		require.NoError(t, applyMigration(db, schema, version, checksum, "unused"))
	})

	t.Run("new migration in custom schema", func(t *testing.T) {
		schema := requireMigrationSchema(t, "open_discogs")
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(
			`lock table "open_discogs"."open_discogs_schema_migration" in exclusive mode`,
		)).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(
			`select checksum from "open_discogs"."open_discogs_schema_migration" where version = $1`,
		)).WithArgs(version).WillReturnRows(sqlmock.NewRows([]string{"checksum"}))
		expectTrigramExtensionSchema(mock, emptyTrigramExtensionRows())
		expectMigrationSearchPath(mock, `"public", "open_discogs"`)
		mock.ExpectExec(regexp.QuoteMeta(`create table "open_discogs".fixture(id integer)`)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(
			`insert into "open_discogs"."open_discogs_schema_migration" (version, checksum) values ($1, $2)`,
		)).WithArgs(version, checksum).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		require.NoError(t, applyMigration(
			db,
			schema,
			version,
			checksum,
			`create table "open_discogs".fixture(id integer)`,
		))
	})
}

func TestSetMigrationSearchPath(t *testing.T) {
	t.Run("extension absent", func(t *testing.T) {
		schema := requireMigrationSchema(t, "open_discogs")
		db, mock, _ := newMockGorm(t)
		expectTrigramExtensionSchema(mock, emptyTrigramExtensionRows())
		expectMigrationSearchPath(mock, `"public", "open_discogs"`)

		require.NoError(t, setMigrationSearchPath(db, schema))
	})

	t.Run("extension in external schema", func(t *testing.T) {
		schema := requireMigrationSchema(t, "open_discogs")
		db, mock, _ := newMockGorm(t)
		expectTrigramExtensionSchema(
			mock,
			trigramExtensionRows("extensions", `"extensions"`),
		)
		expectMigrationSearchPath(mock, `"extensions", "open_discogs", "public"`)

		require.NoError(t, setMigrationSearchPath(db, schema))
	})

	t.Run("extension in target schema", func(t *testing.T) {
		schema := requireMigrationSchema(t, "user")
		db, mock, _ := newMockGorm(t)
		expectTrigramExtensionSchema(
			mock,
			trigramExtensionRows("user", `"user"`),
		)
		expectMigrationSearchPath(mock, `"user", "public"`)

		require.NoError(t, setMigrationSearchPath(db, schema))
	})

	t.Run("public schema is not duplicated", func(t *testing.T) {
		schema := requireMigrationSchema(t, database.DefaultSchemaName)
		db, mock, _ := newMockGorm(t)
		expectTrigramExtensionSchema(
			mock,
			trigramExtensionRows(database.DefaultSchemaName, `"public"`),
		)
		expectMigrationSearchPath(mock, `"public"`)

		require.NoError(t, setMigrationSearchPath(db, schema))
	})

	t.Run("extension inspection failure", func(t *testing.T) {
		schema := requireMigrationSchema(t, "open_discogs")
		db, mock, _ := newMockGorm(t)
		expected := errors.New("fixture")
		mock.ExpectQuery(regexp.QuoteMeta(trigramExtensionSchemaSQL)).
			WillReturnError(expected)

		err := setMigrationSearchPath(db, schema)
		require.ErrorIs(t, err, expected)
		require.ErrorContains(t, err, "inspect pg_trgm extension schema")
	})

	t.Run("set local failure", func(t *testing.T) {
		schema := requireMigrationSchema(t, "open_discogs")
		db, mock, _ := newMockGorm(t)
		expected := errors.New("fixture")
		expectTrigramExtensionSchema(mock, emptyTrigramExtensionRows())
		mock.ExpectExec(regexp.QuoteMeta(
			`set local search_path to "public", "open_discogs"`,
		)).WillReturnError(expected)

		err := setMigrationSearchPath(db, schema)
		require.ErrorIs(t, err, expected)
		require.ErrorContains(t, err, "set schema migration search path")
	})
}

func poisonedGorm(t *testing.T, expected error) *gorm.DB {
	t.Helper()
	db, _, _ := newMockGorm(t)
	db.AddError(expected)
	return db
}

func TestGormErrorPropagationBoundaries(t *testing.T) {
	expected := errors.New("fixture")
	db := poisonedGorm(t, expected)
	order := NewOrder(context.Background(), 1, 1, "unused", db)

	require.ErrorIs(t, writeReferenceEntities(order, []*opendiscogsmodel.Genre{}, nil).Err(), expected)
	require.ErrorIs(t, reconcileIntegerRelation(
		order,
		integerRelation{table: "fixture", parentColumn: "parent", keyColumn: "key"},
		true,
		[]int32{1},
		[]*opendiscogsmodel.ArtistAlias{},
		func(item *opendiscogsmodel.ArtistAlias) int32 { return item.ArtistID },
		func(item *opendiscogsmodel.ArtistAlias) int32 { return item.AliasID },
	).Err(), expected)
	require.ErrorIs(t, reconcileTextRelation(
		order,
		textRelation{table: "fixture", parentColumn: "parent", keyColumn: "key"},
		true,
		[]int32{1},
		[]*opendiscogsmodel.MasterGenre{},
		func(item *opendiscogsmodel.MasterGenre) int32 { return item.MasterID },
		func(item *opendiscogsmodel.MasterGenre) string { return item.Genre },
	).Err(), expected)
	require.ErrorIs(t, reconcileDigestIntegerRelation(
		order,
		digestIntegerRelation{table: "fixture", parentColumn: "parent", keyColumn: "key", identityColumn: "identity"},
		true,
		[]int32{1},
		[]*opendiscogsmodel.ReleaseItemIdentifier{{ReleaseItemID: 1, Hash: 2, IdentitySHA256: make([]byte, 32)}},
		func(item *opendiscogsmodel.ReleaseItemIdentifier) int32 { return item.ReleaseItemID },
		func(item *opendiscogsmodel.ReleaseItemIdentifier) int32 { return item.Hash },
		func(item *opendiscogsmodel.ReleaseItemIdentifier) []byte { return item.IdentitySHA256 },
	).Err(), expected)
	require.ErrorIs(t, reconcileTwoIntegerKeyRelation(
		order,
		twoIntegerKeyRelation{table: "fixture", parentColumn: "parent", firstKeyColumn: "first", secondKeyColumn: "second"},
		true,
		[]int32{1},
		[]*opendiscogsmodel.ReleaseItemWork{{ReleaseItemID: 1, LabelID: 2, Hash: 3}},
		func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.ReleaseItemID },
		func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.LabelID },
		func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.Hash },
	).Err(), expected)
	require.ErrorIs(t, reconcileDigestTwoIntegerKeyRelation(
		order,
		digestTwoIntegerKeyRelation{
			table: "fixture", parentColumn: "parent", firstKeyColumn: "first",
			secondKeyColumn: "second", identityColumn: "identity",
		},
		true,
		[]int32{1},
		[]*opendiscogsmodel.ReleaseItemCreditedArtist{{
			ReleaseItemID: 1, ArtistID: 2, Hash: 3, IdentitySHA256: make([]byte, 32),
		}},
		func(item *opendiscogsmodel.ReleaseItemCreditedArtist) int32 { return item.ReleaseItemID },
		func(item *opendiscogsmodel.ReleaseItemCreditedArtist) int32 { return item.ArtistID },
		func(item *opendiscogsmodel.ReleaseItemCreditedArtist) int32 { return item.Hash },
		func(item *opendiscogsmodel.ReleaseItemCreditedArtist) []byte { return item.IdentitySHA256 },
	).Err(), expected)
	_, err := findExistingRelationRoots(
		order,
		[]int32{1},
		relationRootTable{table: "fixture", parentColumn: "root_id"},
	)
	require.ErrorIs(t, err, expected)
	require.ErrorIs(t, recordCompletedChunk(db, NewTrackedOrder(
		context.Background(), 1, 1, "unused", db, 1, "artist", false,
	), ChunkMetadata{}), expected)
	require.ErrorIs(t, completeEntityProgress(NewTrackedOrder(
		context.Background(), 1, 1, "unused", db, 1, "artist", false,
	), 0, 0), expected)
}

func TestTwoIntegerReconciliationDeleteModes(t *testing.T) {
	db, mock, _ := newMockGorm(t)
	order := NewOrder(context.Background(), 1, 1, "unused", db)
	relation := twoIntegerKeyRelation{
		table: "fixture", parentColumn: "parent", firstKeyColumn: "first", secondKeyColumn: "second",
	}
	parentID := func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.ReleaseItemID }
	firstKey := func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.LabelID }
	secondKey := func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.Hash }

	notDeleted := reconcileTwoIntegerKeyRelation(
		order, relation, false, nil, nil, parentID, firstKey, secondKey,
	)
	require.NoError(t, notDeleted.Err())
	require.Zero(t, notDeleted.Count())

	mock.ExpectExec("delete from fixture current").
		WithArgs("array", "array", "array", "array").
		WillReturnResult(sqlmock.NewResult(0, 2))
	deleted := reconcileTwoIntegerKeyRelation(
		order, relation, true, []int32{1}, nil, parentID, firstKey, secondKey,
	)
	require.NoError(t, deleted.Err())
	require.Equal(t, 2, deleted.Count())
}

func TestProgressTransactionErrorBoundaries(t *testing.T) {
	expected := errors.New("fixture")

	t.Run("active write failure", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		mock.ExpectRollback()
		order := NewTrackedOrder(context.Background(), 1, 1, "unused", db, 1, "artist", false)
		actual := executeActiveRunTransaction(order, func(Order) result.Result {
			return result.NewResult(0, expected)
		})
		require.ErrorIs(t, actual.Err(), expected)
	})

	t.Run("active fence query failure", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		mock.ExpectQuery("select id.*discogs_import_run").WillReturnError(expected)
		mock.ExpectRollback()
		order := NewTrackedOrder(context.Background(), 1, 1, "unused", db, 1, "artist", false)
		actual := executeActiveRunTransaction(order, func(Order) result.Result {
			return result.NewResult(1, nil)
		})
		require.ErrorContains(t, actual.Err(), "fence active import run")
	})

	t.Run("active fence misses run", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		mock.ExpectQuery("select id.*discogs_import_run").
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		mock.ExpectRollback()
		order := NewTrackedOrder(context.Background(), 1, 1, "unused", db, 1, "artist", false)
		actual := executeActiveRunTransaction(order, func(Order) result.Result {
			return result.NewResult(1, nil)
		})
		require.ErrorContains(t, actual.Err(), "run is not active")
	})

	t.Run("resume query failure", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		mock.ExpectQuery("select first_item_index, item_count").WillReturnError(expected)
		mock.ExpectRollback()
		order := NewTrackedOrder(context.Background(), 1, 1, "unused", db, 1, "artist", true)
		actual := executeChunk(order, ChunkMetadata{}, func(Order) result.Result {
			return result.NewResult(1, nil)
		})
		require.ErrorContains(t, actual.Err(), "check artist chunk")
	})

	t.Run("chunk write failure", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		mock.ExpectRollback()
		order := NewOrder(context.Background(), 1, 1, "unused", db)
		actual := executeChunk(order, ChunkMetadata{}, func(Order) result.Result {
			return result.NewResult(0, expected)
		})
		require.ErrorIs(t, actual.Err(), expected)
	})

	t.Run("resume range mismatch", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("select first_item_index, item_count").
			WillReturnRows(sqlmock.NewRows([]string{"first_item_index", "item_count"}).AddRow(9, 9))
		order := NewTrackedOrder(context.Background(), 1, 1, "unused", db, 1, "artist", true)
		completed, err := chunkAlreadyCompleted(db, order, ChunkMetadata{Index: 1, FirstItemIndex: 1, ItemCount: 1})
		require.False(t, completed)
		require.ErrorContains(t, err, "does not match source range")
	})
}

func newMockTransaction(t *testing.T) (sqlmock.Sqlmock, *sql.Tx) {
	t.Helper()
	_, mock, sqlDB := newMockGorm(t)
	mock.ExpectBegin()
	tx, err := sqlDB.Begin()
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectRollback()
		require.NoError(t, tx.Rollback())
	})
	return mock, tx
}

func TestExecutionQueryHelpersPropagateErrors(t *testing.T) {
	expected := errors.New("fixture")
	dump := importDump("artist", "2026-07-01", "a")

	t.Run("mark abandoned", func(t *testing.T) {
		_, tx := newMockTransaction(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorContains(t, markAbandonedRuns(ctx, tx, []string{"artist"}), "recover abandoned")
	})

	t.Run("read checkpoint", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("select dump_date.*discogs_import_checkpoint").WithArgs("artist").WillReturnError(expected)
		require.ErrorContains(t, assertNotDowngrade(context.Background(), tx, []*opendiscogsmodel.DiscogsDump{dump}, false), "read artist")
	})

	t.Run("find successful", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("select candidate_run.id").WithArgs("fingerprint").WillReturnError(expected)
		_, err := findSuccessfulRun(context.Background(), tx, "fingerprint")
		require.ErrorContains(t, err, "find successful")
	})

	t.Run("find resumable", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("select import_run.id").WithArgs("fingerprint", processorName, "version", 1, 1).WillReturnError(expected)
		_, err := findResumableRun(context.Background(), tx, "fingerprint", "version", 1, 1)
		require.ErrorContains(t, err, "find resumable")
	})

	t.Run("prune superseded", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectExec("delete from discogs_import_run_chunk run_chunk").WillReturnError(expected)
		require.ErrorContains(t, pruneSupersededFailedProgress(context.Background(), tx), "prune superseded")
	})

	t.Run("record dump", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("insert into discogs_dump").WithArgs(
			dump.ETag, dump.DumpDate, dump.EntityType, dump.ChecksumSHA256, dump.SizeBytes, dump.URI,
		).WillReturnError(expected)
		_, err := findOrInsertDump(context.Background(), tx, dump)
		require.ErrorContains(t, err, "record artist dump provenance")
	})

	t.Run("resolve existing dump", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("insert into discogs_dump").WithArgs(
			dump.ETag, dump.DumpDate, dump.EntityType, dump.ChecksumSHA256, dump.SizeBytes, dump.URI,
		).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		mock.ExpectQuery("select id.*from discogs_dump").WithArgs(
			dump.DumpDate, dump.EntityType, dump.ChecksumSHA256,
		).WillReturnError(expected)
		_, err := findOrInsertDump(context.Background(), tx, dump)
		require.ErrorContains(t, err, "resolve artist dump provenance")
	})

	t.Run("insert run", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("insert into discogs_import_run").WithArgs(
			"fingerprint", false, false, processorName, "version", sqlmock.AnyArg(),
		).WillReturnError(expected)
		_, err := insertImportRun(context.Background(), tx, "fingerprint", false, false, "version", 7)
		require.ErrorContains(t, err, "start import run")
	})
}

func TestCopyResumeProgressErrorBoundaries(t *testing.T) {
	expected := errors.New("fixture")
	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  string
	}{
		{
			name: "summary execution",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(2, 1).WillReturnError(expected)
			},
			want: "copy import run 1 summaries",
		},
		{
			name: "summary count",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(2, 1).WillReturnResult(sqlmock.NewErrorResult(expected))
			},
			want: "count copied import run 1 summaries",
		},
		{
			name: "summary mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 0))
			},
			want: "copied 0 of 1 entities",
		},
		{
			name: "chunk execution",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("insert into discogs_import_run_chunk").WithArgs(2, 1).WillReturnError(expected)
			},
			want: "copy import run 1 chunks",
		},
		{
			name: "chunk count",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("insert into discogs_import_run_chunk").WithArgs(2, 1).WillReturnResult(sqlmock.NewErrorResult(expected))
			},
			want: "count copied import run 1 chunks",
		},
		{
			name: "prune execution",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("insert into discogs_import_run_chunk").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("delete from discogs_import_run_chunk").WithArgs(1).WillReturnError(expected)
			},
			want: "prune resumed import run 1 chunks",
		},
		{
			name: "prune count",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("insert into discogs_import_run_chunk").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("delete from discogs_import_run_chunk").WithArgs(1).WillReturnResult(sqlmock.NewErrorResult(expected))
			},
			want: "count pruned import run 1 chunks",
		},
		{
			name: "transfer mismatch",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("insert into discogs_import_run_chunk").WithArgs(2, 1).WillReturnResult(sqlmock.NewResult(0, 2))
				mock.ExpectExec("delete from discogs_import_run_chunk").WithArgs(1).WillReturnResult(sqlmock.NewResult(0, 1))
			},
			want: "copied 2 but pruned 1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock, tx := newMockTransaction(t)
			test.setup(mock)
			require.ErrorContains(t, copyResumeProgress(context.Background(), tx, 1, 2, 1), test.want)
		})
	}
}

func preloadConfig(t *testing.T) *koanf.Koanf {
	t.Helper()
	config := koanf.New(".")
	require.NoError(t, config.Set("masters", true))
	require.NoError(t, config.Set("artists", false))
	return config
}

func TestPreloadReferenceIDErrorBoundaries(t *testing.T) {
	expected := errors.New("fixture")

	t.Run("query", func(t *testing.T) {
		_, mock, sqlDB := newMockGorm(t)
		mock.ExpectQuery("SELECT id FROM artist").WillReturnError(expected)
		require.ErrorContains(t, preloadReferenceIDs(context.Background(), sqlDB, preloadConfig(t)), "stream artist")
	})

	t.Run("scan", func(t *testing.T) {
		_, mock, sqlDB := newMockGorm(t)
		mock.ExpectQuery("SELECT id FROM artist").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("not-an-integer"))
		require.ErrorContains(t, preloadReferenceIDs(context.Background(), sqlDB, preloadConfig(t)), "scan artist")
	})

	t.Run("row iteration", func(t *testing.T) {
		_, mock, sqlDB := newMockGorm(t)
		mock.ExpectQuery("SELECT id FROM artist").WillReturnRows(
			sqlmock.NewRows([]string{"id"}).AddRow(1).RowError(0, expected),
		)
		require.ErrorContains(t, preloadReferenceIDs(context.Background(), sqlDB, preloadConfig(t)), "read artist")
	})

	t.Run("close", func(t *testing.T) {
		originalClose := closeIdentifierRows
		t.Cleanup(func() { closeIdentifierRows = originalClose })
		closeIdentifierRows = func(*sql.Rows) error { return expected }
		_, mock, sqlDB := newMockGorm(t)
		mock.ExpectQuery("SELECT id FROM artist").
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		require.ErrorContains(t, preloadReferenceIDs(context.Background(), sqlDB, preloadConfig(t)), "close artist")
	})
}

func expectEntityLock(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("select pg_try_advisory_lock").
		WithArgs(opendiscogsmanifest.AdvisoryLockNamespace, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"acquired"}).AddRow(true))
}

func expectEntityUnlock(mock sqlmock.Sqlmock) {
	mock.ExpectExec("select pg_advisory_unlock").
		WithArgs(opendiscogsmanifest.AdvisoryLockNamespace, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectMarkAbandoned(mock sqlmock.Sqlmock) {
	mock.ExpectExec("update discogs_import_run import_run").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectNoCheckpoint(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("select dump_date.*discogs_import_checkpoint").
		WithArgs("artist").
		WillReturnRows(sqlmock.NewRows([]string{"dump_date"}))
}

func expectNoSuccessfulRun(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("select candidate_run.id").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "compatible_entity_count", "selected_entity_count", "incompatible_entity_types",
		}).AddRow(nil, 0, 0, ""))
}

func expectInsertedDump(mock sqlmock.Sqlmock, dump *opendiscogsmodel.DiscogsDump) {
	mock.ExpectQuery("insert into discogs_dump").WithArgs(
		dump.ETag,
		dump.DumpDate,
		dump.EntityType,
		dump.ChecksumSHA256,
		dump.SizeBytes,
		dump.URI,
	).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
}

func expectNoResumableRun(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("select import_run.id").
		WithArgs(sqlmock.AnyArg(), processorName, "version", 1, 5).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
}

func expectInsertedRun(mock sqlmock.Sqlmock, resumedFrom interface{}) {
	mock.ExpectQuery("insert into discogs_import_run").WithArgs(
		sqlmock.AnyArg(),
		false,
		false,
		processorName,
		"version",
		resumedFrom,
	).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(2))
}

func expectInsertedRunDump(mock sqlmock.Sqlmock) {
	mock.ExpectExec("insert into discogs_import_run_dump").
		WithArgs(int64(2), "artist", int64(1), 5, importContractRevision(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestImportCoordinatorValidationBoundaries(t *testing.T) {
	expected := errors.New("fixture")
	coordinator := NewImportExecutionCoordinator(nil, " ")
	require.Equal(t, "development", coordinator.processorVersion)
	require.NoError(t, coordinator.Complete(context.Background(), nil))
	require.ErrorContains(t, func() error {
		_, err := coordinator.Prepare(context.Background(), nil, 1, false, false)
		return err
	}(), "at least one dump")
	require.ErrorContains(t, func() error {
		_, err := coordinator.Prepare(context.Background(), []*opendiscogsmodel.DiscogsDump{importDump("artist", "2026-07-01", "a")}, 0, false, false)
		return err
	}(), "chunk size")
	require.ErrorContains(t, func() error {
		_, err := coordinator.Prepare(context.Background(), []*opendiscogsmodel.DiscogsDump{nil}, 1, false, false)
		return err
	}(), "nil dump")

	originalFingerprint := fingerprintImportManifest
	originalOrder := orderImportEntityTypes
	originalLocks := requiredImportLockTypes
	t.Cleanup(func() {
		fingerprintImportManifest = originalFingerprint
		orderImportEntityTypes = originalOrder
		requiredImportLockTypes = originalLocks
	})
	dump := importDump("artist", "2026-07-01", "a")
	fingerprintImportManifest = func([]opendiscogsmanifest.Dump) (string, error) { return "", expected }
	_, err := coordinator.Prepare(context.Background(), []*opendiscogsmodel.DiscogsDump{dump}, 1, false, false)
	require.ErrorContains(t, err, "fingerprint import manifest")

	fingerprintImportManifest = originalFingerprint
	orderImportEntityTypes = func([]string) ([]string, error) { return nil, expected }
	_, err = coordinator.Prepare(context.Background(), []*opendiscogsmodel.DiscogsDump{dump}, 1, false, false)
	require.ErrorIs(t, err, expected)

	orderImportEntityTypes = originalOrder
	requiredImportLockTypes = func([]string) ([]string, error) { return nil, expected }
	_, err = coordinator.Prepare(context.Background(), []*opendiscogsmodel.DiscogsDump{dump}, 1, false, false)
	require.ErrorIs(t, err, expected)

	_, mock, sqlDB := newMockGorm(t)
	conn, err := sqlDB.Conn(context.Background())
	require.NoError(t, err)
	prepared := NewImportExecutionCoordinator(sqlDB, "version")
	prepared.conn = conn
	_, err = prepared.Prepare(context.Background(), []*opendiscogsmodel.DiscogsDump{dump}, 1, false, false)
	require.ErrorContains(t, err, "already been prepared")
	prepared.release(context.Background())
	prepared.release(context.Background())

	conn, err = sqlDB.Conn(context.Background())
	require.NoError(t, err)
	prepared.conn = conn
	require.Error(t, prepared.acquireEntityLocks(context.Background(), []string{"invalid"}))
	prepared.release(context.Background())

	conn, err = sqlDB.Conn(context.Background())
	require.NoError(t, err)
	prepared.conn = conn
	mock.ExpectQuery("select pg_try_advisory_lock").WithArgs(
		opendiscogsmanifest.AdvisoryLockNamespace,
		sqlmock.AnyArg(),
	).WillReturnError(expected)
	require.ErrorContains(t, prepared.acquireEntityLocks(context.Background(), []string{"artist"}), "acquire artist import lock")
	prepared.release(context.Background())
}

func TestImportCoordinatorReserveConnectionFailure(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectClose()
	require.NoError(t, sqlDB.Close())
	_, err = NewImportExecutionCoordinator(sqlDB, "version").Prepare(
		context.Background(),
		[]*opendiscogsmodel.DiscogsDump{importDump("artist", "2026-07-01", "a")},
		5,
		false,
		false,
	)
	require.ErrorContains(t, err, "reserve import lock connection")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestImportCoordinatorAdmissionFailures(t *testing.T) {
	expected := errors.New("fixture")
	dump := importDump("artist", "2026-07-01", "a")
	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  string
	}{
		{
			name: "begin",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin().WillReturnError(expected)
				expectEntityUnlock(mock)
			},
			want: "begin import admission",
		},
		{
			name: "mark abandoned",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				mock.ExpectExec("update discogs_import_run import_run").WithArgs(sqlmock.AnyArg()).WillReturnError(expected)
				mock.ExpectRollback()
				expectEntityUnlock(mock)
			},
			want: "recover abandoned",
		},
		{
			name: "checkpoint",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				mock.ExpectQuery("select dump_date.*discogs_import_checkpoint").WithArgs("artist").WillReturnError(expected)
				mock.ExpectRollback()
				expectEntityUnlock(mock)
			},
			want: "read artist import checkpoint",
		},
		{
			name: "successful lookup",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				expectNoCheckpoint(mock)
				expectInsertedDump(mock, dump)
				mock.ExpectQuery("select candidate_run.id").WithArgs(sqlmock.AnyArg()).WillReturnError(expected)
				mock.ExpectRollback()
				expectEntityUnlock(mock)
			},
			want: "find successful import manifest",
		},
		{
			name: "skip commit",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				expectNoCheckpoint(mock)
				expectInsertedDump(mock, dump)
				mock.ExpectQuery("select candidate_run.id").WithArgs(sqlmock.AnyArg()).WillReturnRows(
					sqlmock.NewRows([]string{
						"id", "compatible_entity_count", "selected_entity_count", "incompatible_entity_types",
					}).AddRow(9, 1, 1, ""),
				)
				mock.ExpectCommit().WillReturnError(expected)
				expectEntityUnlock(mock)
			},
			want: "commit skipped import admission",
		},
		{
			name: "dump provenance",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				expectNoCheckpoint(mock)
				mock.ExpectQuery("insert into discogs_dump").WithArgs(
					dump.ETag, dump.DumpDate, dump.EntityType, dump.ChecksumSHA256, dump.SizeBytes, dump.URI,
				).WillReturnError(expected)
				mock.ExpectRollback()
				expectEntityUnlock(mock)
			},
			want: "record artist dump provenance",
		},
		{
			name: "resume lookup",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				expectNoCheckpoint(mock)
				expectInsertedDump(mock, dump)
				expectNoSuccessfulRun(mock)
				mock.ExpectQuery("select import_run.id").WithArgs(sqlmock.AnyArg(), processorName, "version", 1, 5).WillReturnError(expected)
				mock.ExpectRollback()
				expectEntityUnlock(mock)
			},
			want: "find resumable import run",
		},
		{
			name: "insert run",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				expectNoCheckpoint(mock)
				expectInsertedDump(mock, dump)
				expectNoSuccessfulRun(mock)
				expectNoResumableRun(mock)
				mock.ExpectQuery("insert into discogs_import_run").WithArgs(
					sqlmock.AnyArg(), false, false, processorName, "version", sqlmock.AnyArg(),
				).WillReturnError(expected)
				mock.ExpectRollback()
				expectEntityUnlock(mock)
			},
			want: "start import run",
		},
		{
			name: "insert run dump",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				expectNoCheckpoint(mock)
				expectInsertedDump(mock, dump)
				expectNoSuccessfulRun(mock)
				expectNoResumableRun(mock)
				expectInsertedRun(mock, sqlmock.AnyArg())
				mock.ExpectExec("insert into discogs_import_run_dump").WithArgs(
					int64(2), "artist", int64(1), 5, importContractRevision(1),
				).WillReturnError(expected)
				mock.ExpectRollback()
				expectEntityUnlock(mock)
			},
			want: "record import run dump artist",
		},
		{
			name: "resume copy",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				expectNoCheckpoint(mock)
				expectInsertedDump(mock, dump)
				expectNoSuccessfulRun(mock)
				mock.ExpectQuery("select import_run.id").WithArgs(sqlmock.AnyArg(), processorName, "version", 1, 5).
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
				expectInsertedRun(mock, sqlmock.AnyArg())
				expectInsertedRunDump(mock)
				mock.ExpectExec("update discogs_import_run_dump target").WithArgs(int64(2), int64(7)).WillReturnError(expected)
				mock.ExpectRollback()
				expectEntityUnlock(mock)
			},
			want: "copy import run 7 summaries",
		},
		{
			name: "commit",
			setup: func(mock sqlmock.Sqlmock) {
				expectEntityLock(mock)
				mock.ExpectBegin()
				expectMarkAbandoned(mock)
				expectNoCheckpoint(mock)
				expectInsertedDump(mock, dump)
				expectNoSuccessfulRun(mock)
				expectNoResumableRun(mock)
				expectInsertedRun(mock, sqlmock.AnyArg())
				expectInsertedRunDump(mock)
				mock.ExpectCommit().WillReturnError(expected)
				expectEntityUnlock(mock)
			},
			want: "commit import admission",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, mock, sqlDB := newMockGorm(t)
			test.setup(mock)
			_, err := NewImportExecutionCoordinator(sqlDB, "version").Prepare(
				context.Background(),
				[]*opendiscogsmodel.DiscogsDump{dump},
				5,
				false,
				false,
			)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func preparedMockCoordinator(t *testing.T) (sqlmock.Sqlmock, *ImportExecutionCoordinator) {
	t.Helper()
	_, mock, sqlDB := newMockGorm(t)
	conn, err := sqlDB.Conn(context.Background())
	require.NoError(t, err)
	return mock, &ImportExecutionCoordinator{
		db:               sqlDB,
		processorVersion: "version",
		conn:             conn,
		runID:            1,
	}
}

func expectCompletionValidation(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("select count.*discogs_import_run_dump").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
}

func expectCompletedRunUpdate(mock sqlmock.Sqlmock) {
	mock.ExpectExec("update discogs_import_run").
		WithArgs("success", sqlmock.AnyArg(), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectPrunedSupersededProgress(mock sqlmock.Sqlmock) {
	mock.ExpectExec("delete from discogs_import_run_chunk run_chunk").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func TestImportCoordinatorCompletionFailures(t *testing.T) {
	expected := errors.New("fixture")
	tests := []struct {
		name   string
		setup  func(sqlmock.Sqlmock)
		runErr error
		want   string
	}{
		{
			name: "begin",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(expected)
			},
			want: "begin import completion",
		},
		{
			name: "validation query",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery("select count.*discogs_import_run_dump").WithArgs(int64(1)).WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "validate import run 1 completion",
		},
		{
			name: "status update",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("update discogs_import_run").
					WithArgs("failed", sqlmock.AnyArg(), int64(1)).
					WillReturnError(expected)
				mock.ExpectRollback()
			},
			runErr: expected,
			want:   "complete import run 1",
		},
		{
			name: "status result",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("update discogs_import_run").
					WithArgs("failed", sqlmock.AnyArg(), int64(1)).
					WillReturnResult(sqlmock.NewErrorResult(expected))
				mock.ExpectRollback()
			},
			runErr: expected,
			want:   "read import completion result",
		},
		{
			name: "run no longer active",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("update discogs_import_run").
					WithArgs("failed", sqlmock.AnyArg(), int64(1)).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectRollback()
			},
			runErr: expected,
			want:   "was not running",
		},
		{
			name: "prune superseded",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectCompletionValidation(mock)
				expectCompletedRunUpdate(mock)
				mock.ExpectExec("delete from discogs_import_run_chunk run_chunk").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "prune superseded failed import progress",
		},
		{
			name: "prune completed chunks",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectCompletionValidation(mock)
				expectCompletedRunUpdate(mock)
				expectPrunedSupersededProgress(mock)
				mock.ExpectExec("delete from discogs_import_run_chunk where import_run_id").
					WithArgs(int64(1)).
					WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "prune completed import run 1 chunks",
		},
		{
			name: "commit",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectCompletionValidation(mock)
				expectCompletedRunUpdate(mock)
				expectPrunedSupersededProgress(mock)
				mock.ExpectExec("delete from discogs_import_run_chunk where import_run_id").
					WithArgs(int64(1)).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit().WillReturnError(expected)
			},
			want: "commit import completion",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock, coordinator := preparedMockCoordinator(t)
			test.setup(mock)
			require.ErrorContains(t, coordinator.Complete(context.Background(), test.runErr), test.want)
		})
	}
}
