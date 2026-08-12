package batch

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestImportContractRevisionDefinition(t *testing.T) {
	revisions, err := resolveImportContractRevisions([]string{
		artistEntityType,
		labelEntityType,
		masterEntityType,
		releaseEntityType,
	})
	require.NoError(t, err)
	require.Equal(t, []importContractRevision{1, 1, 1, 3}, revisions)
	require.Equal(
		t,
		"case relation.entity_type when 'artist' then 1 when 'label' then 1 when 'master' then 1 when 'release' then 3 end",
		importContractRevisionSQL("relation.entity_type"),
	)

	_, err = resolveImportContractRevisions([]string{"unknown"})
	require.ErrorContains(t, err, "unavailable for entity unknown")
}

func TestMustImportContractRevisionRejectsUnknownEntity(t *testing.T) {
	require.Panics(t, func() {
		mustImportContractRevision("unknown")
	})
}

func TestFindSuccessfulRunContractCompatibility(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		row  *sqlmock.Rows
		want importCheckpointCompatibility
	}{
		{
			name: "exact current manifest",
			row: sqlmock.NewRows([]string{
				"exact_run_id",
				"compatible_count",
				"selected_count",
				"incompatible_entities",
			}).AddRow(11, 4, 4, ""),
			want: importCheckpointCompatibility{
				ExactRunID:            11,
				CompatibleEntityCount: 4,
				SelectedEntityCount:   4,
			},
		},
		{
			name: "mixed manifest requires only release",
			row: sqlmock.NewRows([]string{
				"exact_run_id",
				"compatible_count",
				"selected_count",
				"incompatible_entities",
			}).AddRow(nil, 3, 4, releaseEntityType),
			want: importCheckpointCompatibility{
				CompatibleEntityCount:   3,
				SelectedEntityCount:     4,
				IncompatibleEntityTypes: []string{releaseEntityType},
			},
		},
		{
			name: "no successful candidate",
			row: sqlmock.NewRows([]string{
				"exact_run_id",
				"compatible_count",
				"selected_count",
				"incompatible_entities",
			}).AddRow(nil, 0, 0, ""),
			want: importCheckpointCompatibility{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock, tx := newMockTransaction(t)
			mock.ExpectQuery("select candidate.id").
				WithArgs("fingerprint").
				WillReturnRows(test.row)

			actual, err := findSuccessfulRun(ctx, tx, "fingerprint")
			require.NoError(t, err)
			require.Equal(t, test.want, actual)
		})
	}
}

func TestFindResumableRunUsesEntityContractRevisions(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name string
		rows *sqlmock.Rows
		want int64
	}{
		{
			name: "current revisions",
			rows: sqlmock.NewRows([]string{"id"}).AddRow(12),
			want: 12,
		},
		{
			name: "no compatible run",
			rows: sqlmock.NewRows([]string{"id"}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock, tx := newMockTransaction(t)
			mock.ExpectQuery(regexp.QuoteMeta(
				"run_dump.import_contract_revision is distinct from case run_dump.entity_type",
			)).WithArgs(
				"fingerprint",
				processorName,
				"version",
				4,
				5,
			).WillReturnRows(test.rows)

			runID, err := findResumableRun(ctx, tx, "fingerprint", "version", 5, 4)
			require.NoError(t, err)
			require.Equal(t, test.want, runID)
		})
	}
}

func TestConsolidateSuccessfulImportRun(t *testing.T) {
	ctx := context.Background()
	mock, tx := newMockTransaction(t)
	mock.ExpectQuery("insert into discogs_import_run").WithArgs(
		"fingerprint",
		processorName,
		"version",
	).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(13))
	mock.ExpectExec(regexp.QuoteMeta(
		"total_chunks, import_contract_revision)",
	)).WithArgs(
		int64(13),
		"fingerprint",
	).WillReturnResult(sqlmock.NewResult(0, 4))

	runID, err := consolidateSuccessfulImportRun(
		ctx,
		tx,
		"fingerprint",
		"version",
		4,
	)
	require.NoError(t, err)
	require.Equal(t, int64(13), runID)
}

func TestImportContractRevisionErrorBoundaries(t *testing.T) {
	ctx := context.Background()
	expected := errors.New("fixture")
	missingColumn := &pgconn.PgError{
		Code:    undefinedColumnSQLState,
		Message: "column import_contract_revision does not exist",
	}

	t.Run("successful lookup missing V009", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("select candidate.id").
			WithArgs("fingerprint").
			WillReturnError(missingColumn)

		_, err := findSuccessfulRun(ctx, tx, "fingerprint")
		require.ErrorIs(t, err, missingColumn)
		require.ErrorContains(t, err, importContractRevisionColumnReference)
		require.ErrorContains(t, err, importContractRevisionMigration)
	})

	t.Run("resumable lookup", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("select import_run.id").WithArgs(
			"fingerprint",
			processorName,
			"version",
			1,
			5,
		).WillReturnError(expected)

		_, err := findResumableRun(ctx, tx, "fingerprint", "version", 5, 1)
		require.ErrorIs(t, err, expected)
		require.ErrorContains(t, err, "find resumable import run")
	})

	t.Run("consolidated run insert", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("insert into discogs_import_run").WithArgs(
			"fingerprint",
			processorName,
			"version",
		).WillReturnError(expected)

		_, err := consolidateSuccessfulImportRun(
			ctx,
			tx,
			"fingerprint",
			"version",
			1,
		)
		require.ErrorIs(t, err, expected)
		require.ErrorContains(t, err, "record consolidated successful import run")
	})

	for _, test := range []struct {
		name   string
		result sql.Result
		want   string
	}{
		{
			name:   "consolidated run dump count",
			result: sqlmock.NewErrorResult(expected),
			want:   "count consolidated successful import run dumps",
		},
		{
			name:   "consolidated run dump mismatch",
			result: sqlmock.NewResult(0, 0),
			want:   "recorded 0 of 1 entities",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock, tx := newMockTransaction(t)
			mock.ExpectQuery("insert into discogs_import_run").WithArgs(
				"fingerprint",
				processorName,
				"version",
			).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(13))
			mock.ExpectExec("insert into discogs_import_run_dump").WithArgs(
				int64(13),
				"fingerprint",
			).WillReturnResult(test.result)

			_, err := consolidateSuccessfulImportRun(
				ctx,
				tx,
				"fingerprint",
				"version",
				1,
			)
			require.ErrorContains(t, err, test.want)
		})
	}

	t.Run("consolidated run dump missing V009", func(t *testing.T) {
		mock, tx := newMockTransaction(t)
		mock.ExpectQuery("insert into discogs_import_run").WithArgs(
			"fingerprint",
			processorName,
			"version",
		).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(13))
		mock.ExpectExec("insert into discogs_import_run_dump").WithArgs(
			int64(13),
			"fingerprint",
		).WillReturnError(missingColumn)

		_, err := consolidateSuccessfulImportRun(
			ctx,
			tx,
			"fingerprint",
			"version",
			1,
		)
		require.ErrorIs(t, err, missingColumn)
		require.ErrorContains(t, err, importContractRevisionColumnReference)
	})

	actual := importContractRevisionQueryError("test operation", expected)
	require.ErrorIs(t, actual, expected)
	require.ErrorContains(t, actual, "test operation")
}

func TestInsertImportRunDoesNotUseProcessorRevision(t *testing.T) {
	for _, test := range []struct {
		name          string
		resumedFromID int64
		resumedFrom   sql.NullInt64
	}{
		{name: "new run"},
		{
			name:          "resumed run",
			resumedFromID: 7,
			resumedFrom:   sql.NullInt64{Int64: 7, Valid: true},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock, tx := newMockTransaction(t)
			mock.ExpectQuery("insert into discogs_import_run").WithArgs(
				"fingerprint",
				true,
				true,
				processorName,
				"version",
				test.resumedFrom,
			).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(14))

			runID, err := insertImportRun(
				context.Background(),
				tx,
				"fingerprint",
				true,
				true,
				"version",
				test.resumedFromID,
			)
			require.NoError(t, err)
			require.Equal(t, int64(14), runID)
		})
	}

	expected := errors.New("fixture")
	mock, tx := newMockTransaction(t)
	mock.ExpectQuery("insert into discogs_import_run").WithArgs(
		"fingerprint",
		false,
		false,
		processorName,
		"version",
		sql.NullInt64{},
	).WillReturnError(expected)
	_, err := insertImportRun(
		context.Background(),
		tx,
		"fingerprint",
		false,
		false,
		"version",
		0,
	)
	require.ErrorIs(t, err, expected)
}

func TestInsertImportRunDumpStoresEntityRevision(t *testing.T) {
	ctx := context.Background()
	for entityType, revision := range currentImportContractRevisions {
		t.Run(entityType, func(t *testing.T) {
			mock, tx := newMockTransaction(t)
			mock.ExpectExec("insert into discogs_import_run_dump").WithArgs(
				int64(11),
				entityType,
				int64(12),
				5,
				revision,
			).WillReturnResult(sqlmock.NewResult(0, 1))

			require.NoError(t, insertImportRunDump(
				ctx,
				tx,
				11,
				entityType,
				12,
				5,
				revision,
			))
		})
	}

	missingColumn := &pgconn.PgError{
		Code:    undefinedColumnSQLState,
		Message: "column import_contract_revision does not exist",
	}
	mock, tx := newMockTransaction(t)
	mock.ExpectExec("insert into discogs_import_run_dump").WithArgs(
		int64(11),
		releaseEntityType,
		int64(12),
		5,
		currentImportContractRevisions[releaseEntityType],
	).WillReturnError(missingColumn)

	err := insertImportRunDump(
		ctx,
		tx,
		11,
		releaseEntityType,
		12,
		5,
		currentImportContractRevisions[releaseEntityType],
	)
	require.ErrorIs(t, err, missingColumn)
	require.ErrorContains(t, err, importContractRevisionColumnReference)
}

func TestPruneSupersededProgressUsesEntityContractRevision(t *testing.T) {
	expected := errors.New("fixture")
	for _, test := range []struct {
		name    string
		result  sql.Result
		execErr error
	}{
		{
			name:   "success",
			result: sqlmock.NewResult(0, 1),
		},
		{
			name:    "query failure",
			execErr: expected,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock, tx := newMockTransaction(t)
			expectation := mock.ExpectExec(regexp.QuoteMeta(
				"current_dump.import_contract_revision is distinct from",
			))
			if test.execErr != nil {
				expectation.WillReturnError(test.execErr)
			} else {
				expectation.WillReturnResult(test.result)
			}

			err := pruneSupersededFailedProgress(context.Background(), tx)
			if test.execErr != nil {
				require.ErrorIs(t, err, expected)
				require.ErrorContains(t, err, "prune superseded failed import progress")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
