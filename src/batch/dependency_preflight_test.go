package batch

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const dependencyTestDateLayout = "2006-01-02"

func TestImportDependencyRequirements(t *testing.T) {
	t.Run("artist and label have no reference dependencies", func(t *testing.T) {
		requirements, err := importDependencyRequirements([]*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-08-02", "1"),
			importDump("label", "2026-08-03", "2"),
		})
		require.NoError(t, err)
		require.Empty(t, requirements)
	})

	t.Run("master requires artist", func(t *testing.T) {
		requirements, err := importDependencyRequirements([]*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-08-04", "3"),
		})
		require.NoError(t, err)
		require.Equal(t, []dependencyRequirement{{
			entityType:       "artist",
			requiredBy:       []string{"master"},
			horizonExclusive: dependencyTestDate(t, "2026-09-01"),
		}}, requirements)
	})

	t.Run("release requires artist label and master", func(t *testing.T) {
		requirements, err := importDependencyRequirements([]*opendiscogsmodel.DiscogsDump{
			importDump("release", "2026-08-05", "4"),
		})
		require.NoError(t, err)
		require.Equal(t, []dependencyRequirement{
			{
				entityType:       "artist",
				requiredBy:       []string{"release"},
				horizonExclusive: dependencyTestDate(t, "2026-09-01"),
			},
			{
				entityType:       "label",
				requiredBy:       []string{"release"},
				horizonExclusive: dependencyTestDate(t, "2026-09-01"),
			},
			{
				entityType:       "master",
				requiredBy:       []string{"release"},
				horizonExclusive: dependencyTestDate(t, "2026-09-01"),
			},
		}, requirements)
	})

	t.Run("included dependencies satisfy the plan in execution order", func(t *testing.T) {
		requirements, err := importDependencyRequirements([]*opendiscogsmodel.DiscogsDump{
			importDump("release", "2026-08-05", "4"),
			importDump("master", "2026-07-31", "3"),
			importDump("label", "2026-08-03", "2"),
			importDump("artist", "2026-08-02", "1"),
		})
		require.NoError(t, err)
		require.Empty(t, requirements)
	})

	t.Run("shared dependency uses the latest required month", func(t *testing.T) {
		requirements, err := importDependencyRequirements([]*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-07-31", "3"),
			importDump("release", "2026-08-05", "4"),
			importDump("label", "2026-08-03", "2"),
		})
		require.NoError(t, err)
		require.Equal(t, []dependencyRequirement{{
			entityType:       "artist",
			requiredBy:       []string{"master", "release"},
			horizonExclusive: dependencyTestDate(t, "2026-09-01"),
		}}, requirements)
	})
}

func TestImportDependencyRequirementValidation(t *testing.T) {
	tests := []struct {
		name  string
		dumps []*opendiscogsmodel.DiscogsDump
		want  string
	}{
		{name: "nil dump", dumps: []*opendiscogsmodel.DiscogsDump{nil}, want: "nil dump"},
		{
			name: "unknown entity",
			dumps: []*opendiscogsmodel.DiscogsDump{
				importDump("release", "2026-08-05", "4"),
				{EntityType: "unknown", DumpDate: dependencyTestDate(t, "2026-08-01")},
			},
			want: "unknown entity type",
		},
		{
			name: "duplicate entity",
			dumps: []*opendiscogsmodel.DiscogsDump{
				importDump("artist", "2026-08-01", "1"),
				importDump("ARTIST", "2026-08-02", "2"),
			},
			want: "duplicate entity type",
		},
		{
			name:  "missing date",
			dumps: []*opendiscogsmodel.DiscogsDump{{EntityType: "artist"}},
			want:  "dump date is required",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := importDependencyRequirements(test.dumps)
			require.ErrorContains(t, err, test.want)
		})
	}

	t.Run("dependency graph errors are preserved", func(t *testing.T) {
		expected := errors.New("fixture")
		original := requiredImportDependencyTypes
		requiredImportDependencyTypes = func([]string) ([]string, error) {
			return nil, expected
		}
		t.Cleanup(func() { requiredImportDependencyTypes = original })

		_, err := importDependencyRequirements([]*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-08-04", "3"),
		})

		require.ErrorIs(t, err, expected)
		require.ErrorContains(t, err, "resolve master import dependencies")
	})
}

func TestPreflightImportDependencies(t *testing.T) {
	ctx := context.Background()
	releaseDump := importDump("release", "2026-08-05", "4")
	horizon := dependencyTestDate(t, "2026-09-01")

	t.Run("independent entities and complete plans need no checkpoint query", func(t *testing.T) {
		require.NoError(t, preflightImportDependencies(ctx, nil, []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-08-02", "1"),
			importDump("label", "2026-08-03", "2"),
		}))
		require.NoError(t, preflightImportDependencies(ctx, nil, []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-08-02", "1"),
			importDump("label", "2026-08-03", "2"),
			importDump("master", "2026-07-31", "3"),
			releaseDump,
		}))
	})

	t.Run("required dependencies need a database", func(t *testing.T) {
		err := preflightImportDependencies(ctx, nil, []*opendiscogsmodel.DiscogsDump{releaseDump})
		require.ErrorContains(t, err, "database connection is nil")
	})

	t.Run("invalid plan is rejected before database access", func(t *testing.T) {
		err := preflightImportDependencies(ctx, nil, []*opendiscogsmodel.DiscogsDump{nil})
		require.ErrorContains(t, err, "nil dump")
	})

	t.Run("empty database rejects release-only import", func(t *testing.T) {
		db, mock := dependencySQLMock(t)
		expectDependencyCheckpoint(mock, "artist", horizon).
			WillReturnRows(dependencyRows())

		err := preflightImportDependencies(ctx, db, []*opendiscogsmodel.DiscogsDump{releaseDump})

		require.ErrorContains(t, err, "successful artist checkpoint")
	})

	t.Run("incomplete checkpoint set rejects release-only import", func(t *testing.T) {
		db, mock := dependencySQLMock(t)
		artistDate := dependencyTestDate(t, "2026-07-29")
		expectDependencyCheckpoint(mock, "artist", horizon).
			WillReturnRows(dependencyRows().AddRow(artistDate, checksum("a"), artistDate, checksum("a")))
		expectDependencyCheckpoint(mock, "label", horizon).
			WillReturnRows(dependencyRows())

		err := preflightImportDependencies(ctx, db, []*opendiscogsmodel.DiscogsDump{releaseDump})

		require.ErrorContains(t, err, "successful label checkpoint")
	})

	t.Run("mixed publication dates are compatible", func(t *testing.T) {
		db, mock := dependencySQLMock(t)
		for _, dependency := range []struct {
			entityType string
			date       string
			seed       string
		}{
			{entityType: "artist", date: "2026-07-29", seed: "a"},
			{entityType: "label", date: "2026-08-02", seed: "b"},
			{entityType: "master", date: "2026-07-31", seed: "c"},
		} {
			date := dependencyTestDate(t, dependency.date)
			expectDependencyCheckpoint(mock, dependency.entityType, horizon).
				WillReturnRows(dependencyRows().AddRow(
					date,
					checksum(dependency.seed),
					date,
					strings.ToUpper(checksum(dependency.seed)),
				))
		}

		require.NoError(t, preflightImportDependencies(
			ctx,
			db,
			[]*opendiscogsmodel.DiscogsDump{releaseDump},
		))
	})

	t.Run("a newer checkpoint is compatible without an older catalog candidate", func(t *testing.T) {
		db, mock := dependencySQLMock(t)
		expectDependencyCheckpoint(mock, "artist", horizon).
			WillReturnRows(dependencyRows().AddRow(
				dependencyTestDate(t, "2026-09-02"),
				checksum("a"),
				nil,
				nil,
			))

		require.NoError(t, preflightImportDependencies(
			ctx,
			db,
			[]*opendiscogsmodel.DiscogsDump{importDump("master", "2026-08-04", "3")},
		))
	})

	t.Run("known newer dependency dump makes checkpoint stale", func(t *testing.T) {
		db, mock := dependencySQLMock(t)
		expectDependencyCheckpoint(mock, "artist", horizon).
			WillReturnRows(dependencyRows().AddRow(
				dependencyTestDate(t, "2026-07-29"),
				checksum("a"),
				dependencyTestDate(t, "2026-08-04"),
				checksum("d"),
			))

		err := preflightImportDependencies(ctx, db, []*opendiscogsmodel.DiscogsDump{releaseDump})

		require.ErrorContains(t, err, "artist checkpoint 2026-07-29 is stale")
	})

	t.Run("query errors are explicit", func(t *testing.T) {
		db, mock := dependencySQLMock(t)
		expected := errors.New("fixture")
		expectDependencyCheckpoint(mock, "artist", horizon).WillReturnError(expected)

		err := preflightImportDependencies(ctx, db, []*opendiscogsmodel.DiscogsDump{releaseDump})

		require.ErrorIs(t, err, expected)
		require.ErrorContains(t, err, "read artist dependency checkpoint")
	})
}

func TestValidateDependencyCheckpoint(t *testing.T) {
	requirement := dependencyRequirement{
		entityType:       "artist",
		requiredBy:       []string{"release"},
		horizonExclusive: dependencyTestDate(t, "2026-09-01"),
	}

	t.Run("newer checkpoint is compatible without historical candidate", func(t *testing.T) {
		err := validateDependencyCheckpoint(requirement, dependencyCheckpoint{
			dumpDate:       dependencyTestDate(t, "2026-09-02"),
			checksumSHA256: checksum("a"),
		}, nil)
		require.NoError(t, err)
	})

	t.Run("missing catalog provenance is rejected", func(t *testing.T) {
		err := validateDependencyCheckpoint(requirement, dependencyCheckpoint{
			dumpDate:       dependencyTestDate(t, "2026-07-29"),
			checksumSHA256: checksum("a"),
		}, nil)
		require.ErrorContains(t, err, "no immutable catalog provenance")
	})

	t.Run("same-date reissue requires the current checksum", func(t *testing.T) {
		date := dependencyTestDate(t, "2026-08-02")
		err := validateDependencyCheckpoint(
			requirement,
			dependencyCheckpoint{dumpDate: date, checksumSHA256: checksum("a")},
			&dependencyCheckpoint{dumpDate: date, checksumSHA256: checksum("b")},
		)
		require.ErrorContains(t, err, "is stale")
	})

	t.Run("checkpoint newer than the expected dump is compatible", func(t *testing.T) {
		err := validateDependencyCheckpoint(
			requirement,
			dependencyCheckpoint{
				dumpDate:       dependencyTestDate(t, "2026-08-03"),
				checksumSHA256: checksum("a"),
			},
			&dependencyCheckpoint{
				dumpDate:       dependencyTestDate(t, "2026-08-02"),
				checksumSHA256: checksum("b"),
			},
		)
		require.NoError(t, err)
	})
}

func TestDependencyPreflightUsesCanonicalPostgreSQLCheckpoints(t *testing.T) {
	ctx := context.Background()
	pg := testutils.GetDatabase(t, testutils.Postgres)
	dsn := testutils.GetDsn(testutils.Postgres, pg)
	db, err := database.GetConnect(dsn)
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))
	db, err = database.GetConnect(dsn)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)

	masterDump := importDump("master", "2026-08-04", "3")
	releaseDump := importDump("release", "2026-08-05", "4")
	require.ErrorContains(t, preflightImportDependencies(
		ctx,
		sqlDB,
		[]*opendiscogsmodel.DiscogsDump{masterDump},
	), "successful artist checkpoint")
	require.ErrorContains(t, preflightImportDependencies(
		ctx,
		sqlDB,
		[]*opendiscogsmodel.DiscogsDump{releaseDump},
	), "successful artist checkpoint")

	require.NoError(t, preflightImportDependencies(ctx, sqlDB, []*opendiscogsmodel.DiscogsDump{
		importDump("artist", "2026-08-02", "1"),
		importDump("label", "2026-08-03", "2"),
	}))
	require.NoError(t, preflightImportDependencies(ctx, sqlDB, []*opendiscogsmodel.DiscogsDump{
		importDump("artist", "2026-08-02", "1"),
		importDump("label", "2026-08-03", "2"),
		masterDump,
		releaseDump,
	}))

	recordSuccessfulDependencyCheckpoint(t, db, importDump("artist", "2026-07-29", "a"))
	require.ErrorContains(t, preflightImportDependencies(
		ctx,
		sqlDB,
		[]*opendiscogsmodel.DiscogsDump{releaseDump},
	), "successful label checkpoint")

	recordSuccessfulDependencyCheckpoint(t, db, importDump("label", "2026-08-02", "b"))
	recordSuccessfulDependencyCheckpoint(t, db, importDump("master", "2026-07-31", "c"))
	require.NoError(t, preflightImportDependencies(
		ctx,
		sqlDB,
		[]*opendiscogsmodel.DiscogsDump{releaseDump},
	))

	newerArtistDump := importDump("artist", "2026-08-04", "d")
	require.NoError(t, db.Omit("ID", "CreatedAt", "LastModifiedAt").Create(newerArtistDump).Error)
	require.ErrorContains(t, preflightImportDependencies(
		ctx,
		sqlDB,
		[]*opendiscogsmodel.DiscogsDump{releaseDump},
	), "artist checkpoint 2026-07-29 is stale")
}

func recordSuccessfulDependencyCheckpoint(
	t *testing.T,
	db *gorm.DB,
	dump *opendiscogsmodel.DiscogsDump,
) {
	t.Helper()
	require.NoError(t, db.Omit("ID", "CreatedAt", "LastModifiedAt").Create(dump).Error)
	var runID int64
	require.NoError(t, db.Raw(`
		insert into discogs_import_run
		       (manifest_sha256, status, completed_at, processor, processor_version)
		values (?, 'success', now(), 'go-open-discogs-batch', 'dependency-preflight-test')
		returning id`, dump.ChecksumSHA256).Scan(&runID).Error)
	require.NoError(t, db.Exec(`
		insert into discogs_import_run_dump
		       (import_run_id, entity_type, dump_id, processed_items,
		        last_progress_at, completed_at, chunk_size, total_items, total_chunks,
		        import_contract_revision)
		values (?, ?, ?, 0, now(), now(), 1, 0, 0, ?)`,
		runID,
		dump.EntityType,
		dump.ID,
		currentImportContractRevisions[dump.EntityType],
	).Error)
}

func dependencySQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
	return db, mock
}

func expectDependencyCheckpoint(
	mock sqlmock.Sqlmock,
	entityType string,
	horizon time.Time,
) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery(regexp.QuoteMeta(dependencyCheckpointQuery)).
		WithArgs(entityType, horizon)
}

func dependencyRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"checkpoint_dump_date",
		"checkpoint_checksum_sha256",
		"expected_dump_date",
		"expected_checksum_sha256",
	})
}

func dependencyTestDate(t *testing.T, value string) time.Time {
	t.Helper()
	date, err := time.Parse(dependencyTestDateLayout, value)
	require.NoError(t, err)
	return date
}

func checksum(seed string) string {
	return strings.Repeat(seed, 64)
}
