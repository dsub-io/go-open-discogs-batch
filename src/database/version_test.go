package database

import (
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestRequireSupportedPostgreSQLServerVersion(t *testing.T) {
	tests := []struct {
		name        string
		version     postgreSQLServerVersionNumber
		wantError   string
		wantNoError bool
	}{
		{
			name:      "reject PostgreSQL 14",
			version:   140_012,
			wantError: "PostgreSQL 14 is unsupported (server_version_num=140012): open-discogs-model v0.2.3 requires PostgreSQL 15 or newer; upgrade PostgreSQL before starting the import",
		},
		{name: "accept PostgreSQL 15", version: 150_000, wantNoError: true},
		{name: "accept PostgreSQL 18", version: 180_001, wantNoError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := requireSupportedPostgreSQLServerVersion(test.version)
			if test.wantNoError {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.wantError)
		})
	}
}

func TestValidatePostgreSQLServerVersion(t *testing.T) {
	t.Run("accept supported server", func(t *testing.T) {
		db, mock := newSchemaMock(t)
		mock.ExpectQuery(regexp.QuoteMeta(postgreSQLServerVersionQuery)).
			WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow(180_001))

		require.NoError(t, validatePostgreSQLServerVersion(db))
	})

	t.Run("report version query failure", func(t *testing.T) {
		db, mock := newSchemaMock(t)
		expected := errors.New("version unavailable")
		mock.ExpectQuery(regexp.QuoteMeta(postgreSQLServerVersionQuery)).WillReturnError(expected)

		err := validatePostgreSQLServerVersion(db)
		require.ErrorContains(t, err, "read PostgreSQL server version")
		require.ErrorIs(t, err, expected)
	})
}
