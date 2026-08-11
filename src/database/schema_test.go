package database

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func newSchemaMock(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
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
	return db, mock
}

func requireSchema(t *testing.T, name string) Schema {
	t.Helper()
	schema, err := ParseSchema(name)
	require.NoError(t, err)
	return schema
}

func TestSchemaContract(t *testing.T) {
	publicSchema := requireSchema(t, DefaultSchemaName)
	require.Equal(t, DefaultSchemaName, publicSchema.Name())
	require.Equal(t, `"public"`, publicSchema.Identifier())
	require.Equal(t, `"public"`, publicSchema.SearchPath())
	require.Equal(t, `"public"."artist"`, publicSchema.Qualify("artist"))
	require.Equal(t, `create table "public".artist(id integer)`, publicSchema.ScopeCanonicalSQL(
		`create table public.artist(id integer)`,
	))
	require.NoError(t, ValidateSchemaName(DefaultSchemaName))

	customSchema := requireSchema(t, "open_discogs")
	require.Equal(t, `"open_discogs", "public"`, customSchema.SearchPath())
	require.Equal(t, `"open_discogs".artist`, customSchema.ScopeCanonicalSQL(`public.artist`))

	for _, invalid := range []string{"", "OpenDiscogs", "open-discogs", "1schema", strings.Repeat("a", 64)} {
		require.ErrorContains(t, ValidateSchemaName(invalid), "database-schema")
	}
	require.NoError(t, ValidateSchemaName("_"+strings.Repeat("a", 62)))
}

func TestEnsureSchemaRejectsNilDatabase(t *testing.T) {
	_, err := EnsureSchema(nil, requireSchema(t, "open_discogs"))
	require.ErrorContains(t, err, "connection is nil")
}

func TestEnsureSchemaExisting(t *testing.T) {
	db, mock := newSchemaMock(t)
	mock.ExpectQuery(regexp.QuoteMeta("select exists(select 1 from pg_namespace where nspname = $1)")).
		WithArgs("open_discogs").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta(
		"select has_schema_privilege(current_user, $1, 'USAGE') and has_schema_privilege(current_user, $2, 'CREATE')",
	)).WithArgs("open_discogs", "open_discogs").
		WillReturnRows(sqlmock.NewRows([]string{"allowed"}).AddRow(true))

	created, err := EnsureSchema(db, requireSchema(t, "open_discogs"))
	require.NoError(t, err)
	require.False(t, created)
}

func TestEnsureSchemaCreatesMissingSchema(t *testing.T) {
	db, mock := newSchemaMock(t)
	mock.ExpectQuery(regexp.QuoteMeta("select exists(select 1 from pg_namespace where nspname = $1)")).
		WithArgs("open_discogs").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectQuery(regexp.QuoteMeta(
		"select current_database() as name, has_database_privilege(current_user, current_database(), 'CREATE') as allowed",
	)).WillReturnRows(sqlmock.NewRows([]string{"name", "allowed"}).AddRow("discogs", true))
	mock.ExpectExec(regexp.QuoteMeta(`create schema if not exists "open_discogs" authorization current_user`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(
		"select has_schema_privilege(current_user, $1, 'USAGE') and has_schema_privilege(current_user, $2, 'CREATE')",
	)).WithArgs("open_discogs", "open_discogs").
		WillReturnRows(sqlmock.NewRows([]string{"allowed"}).AddRow(true))

	created, err := EnsureSchema(db, requireSchema(t, "open_discogs"))
	require.NoError(t, err)
	require.True(t, created)
}

func TestEnsureSchemaReportsEveryFailure(t *testing.T) {
	expected := errors.New("fixture")
	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  string
	}{
		{
			name: "namespace inspection",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("select exists").WillReturnError(expected)
			},
			want: "inspect database schema",
		},
		{
			name: "database privilege inspection",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("select exists").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
				mock.ExpectQuery("select current_database").WillReturnError(expected)
			},
			want: "inspect CREATE privilege",
		},
		{
			name: "database privilege denied",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("select exists").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
				mock.ExpectQuery("select current_database").WillReturnRows(
					sqlmock.NewRows([]string{"name", "allowed"}).AddRow("discogs", false),
				)
			},
			want: "pre-create the schema",
		},
		{
			name: "schema creation",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("select exists").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
				mock.ExpectQuery("select current_database").WillReturnRows(
					sqlmock.NewRows([]string{"name", "allowed"}).AddRow("discogs", true),
				)
				mock.ExpectExec("create schema").WillReturnError(expected)
			},
			want: "create database schema",
		},
		{
			name: "schema privilege inspection",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("select exists").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				mock.ExpectQuery("select has_schema_privilege").WillReturnError(expected)
			},
			want: "inspect privileges",
		},
		{
			name: "schema privileges denied",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("select exists").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
				mock.ExpectQuery("select has_schema_privilege").WillReturnRows(
					sqlmock.NewRows([]string{"allowed"}).AddRow(false),
				)
			},
			want: "USAGE and CREATE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock := newSchemaMock(t)
			test.setup(mock)
			_, err := EnsureSchema(db, requireSchema(t, "open_discogs"))
			require.ErrorContains(t, err, test.want)
		})
	}
}
