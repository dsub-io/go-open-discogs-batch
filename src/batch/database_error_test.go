package batch

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io/fs"
	"regexp"
	"testing"
	"testing/fstest"

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
	case pgtype.Array[int32], pgtype.Array[string]:
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
		mock.ExpectClose()
		require.NoError(t, sqlDB.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
	return db, mock, sqlDB
}

func TestRunDDLErrorBoundaries(t *testing.T) {
	schema, schemaErr := database.ParseSchema(database.DefaultSchemaName)
	require.NoError(t, schemaErr)
	originalLoad := loadSchemaMigrations
	t.Cleanup(func() { loadSchemaMigrations = originalLoad })
	expected := errors.New("fixture")
	loadSchemaMigrations = func() (fs.FS, error) { return nil, expected }
	require.ErrorContains(t, RunDDL(nil), "load shared schema migrations")
	loadSchemaMigrations = originalLoad
	require.ErrorContains(t, RunDDLInSchema(nil, "Invalid"), "database-schema")

	db, mock, _ := newMockGorm(t)
	mock.ExpectExec(regexp.QuoteMeta(`create table if not exists "public"."open_discogs_schema_migration"`)).
		WillReturnError(expected)
	require.ErrorContains(t, runDDL(db, fstest.MapFS{}, schema), "create schema migration ledger")
}

func TestRunDDLReadFailure(t *testing.T) {
	schema, schemaErr := database.ParseSchema(database.DefaultSchemaName)
	require.NoError(t, schemaErr)
	db, mock, _ := newMockGorm(t)
	mock.ExpectExec(regexp.QuoteMeta(`create table if not exists "public"."open_discogs_schema_migration"`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	migrations := fstest.MapFS{
		"001-broken.sql": &fstest.MapFile{Mode: fs.ModeDir},
	}
	require.ErrorContains(t, runDDL(db, migrations, schema), "read shared migration")
}

func TestRunDDLPropagatesMigrationFailure(t *testing.T) {
	schema, schemaErr := database.ParseSchema(database.DefaultSchemaName)
	require.NoError(t, schemaErr)
	db, mock, _ := newMockGorm(t)
	expected := errors.New("fixture")
	mock.ExpectExec(regexp.QuoteMeta(`create table if not exists "public"."open_discogs_schema_migration"`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration"`)).WillReturnError(expected)
	mock.ExpectRollback()
	require.ErrorContains(t, runDDL(db, fstest.MapFS{
		"001.sql": &fstest.MapFile{Data: []byte("select 1")},
	}, schema), "read migration ledger")
}

func TestApplyMigrationErrorBoundaries(t *testing.T) {
	expected := errors.New("fixture")
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
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration"`)).WillReturnError(expected)
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
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration"`)).
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
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration"`)).
					WillReturnRows(sqlmock.NewRows([]string{"checksum"}))
				mock.ExpectExec(regexp.QuoteMeta("invalid SQL")).WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "apply shared migration",
		},
		{
			name: "ledger insert failure",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration"`)).
					WillReturnRows(sqlmock.NewRows([]string{"checksum"}))
				mock.ExpectExec(regexp.QuoteMeta("invalid SQL")).WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec(regexp.QuoteMeta(`insert into "public"."open_discogs_schema_migration"`)).WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "record shared migration",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, schemaErr := database.ParseSchema(database.DefaultSchemaName)
			require.NoError(t, schemaErr)
			db, mock, _ := newMockGorm(t)
			test.setup(mock)
			require.ErrorContains(t, applyMigration(db, schema, "001.sql", "new", "invalid SQL"), test.want)
		})
	}

	t.Run("already applied checksum", func(t *testing.T) {
		schema, schemaErr := database.ParseSchema(database.DefaultSchemaName)
		require.NoError(t, schemaErr)
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(`lock table "public"."open_discogs_schema_migration" in exclusive mode`)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(`select checksum from "public"."open_discogs_schema_migration"`)).
			WillReturnRows(sqlmock.NewRows([]string{"checksum"}).AddRow("same"))
		mock.ExpectCommit()
		require.NoError(t, applyMigration(db, schema, "001.sql", "same", "unused"))
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
	require.ErrorIs(t, reconcileTwoIntegerKeyRelation(
		order,
		twoIntegerKeyRelation{table: "fixture", parentColumn: "parent", firstKeyColumn: "first", secondKeyColumn: "second"},
		true,
		[]int32{1},
		[]*opendiscogsmodel.ReleaseItemWork{},
		func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.ReleaseItemID },
		func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.LabelID },
		func(item *opendiscogsmodel.ReleaseItemWork) int32 { return item.Hash },
	).Err(), expected)
	_, err := relationTablesContainRows(order, "fixture")
	require.ErrorIs(t, err, expected)
	require.ErrorIs(t, recordCompletedChunk(db, NewTrackedOrder(
		context.Background(), 1, 1, "unused", db, 1, "artist", false,
	), ChunkMetadata{}), expected)
	require.ErrorIs(t, completeEntityProgress(NewTrackedOrder(
		context.Background(), 1, 1, "unused", db, 1, "artist", false,
	), 0, 0), expected)
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
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
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
		WithArgs(int64(2), "artist", int64(1), 5).
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
				mock.ExpectQuery("select candidate_run.id").WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(9))
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
				expectNoSuccessfulRun(mock)
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
				expectNoSuccessfulRun(mock)
				expectInsertedDump(mock, dump)
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
				expectNoSuccessfulRun(mock)
				expectInsertedDump(mock, dump)
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
				expectNoSuccessfulRun(mock)
				expectInsertedDump(mock, dump)
				expectNoResumableRun(mock)
				expectInsertedRun(mock, sqlmock.AnyArg())
				mock.ExpectExec("insert into discogs_import_run_dump").WithArgs(int64(2), "artist", int64(1), 5).WillReturnError(expected)
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
				expectNoSuccessfulRun(mock)
				expectInsertedDump(mock, dump)
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
				expectNoSuccessfulRun(mock)
				expectInsertedDump(mock, dump)
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
