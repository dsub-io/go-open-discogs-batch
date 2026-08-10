package batch

import (
	"context"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	progressFailureFunction = "fail_selected_import_chunk"
	progressFailureTrigger  = "fail_selected_import_chunk_trigger"
)

func TestImportResumesOutOfOrderChunks(t *testing.T) {
	const (
		chunkSize  = 1
		maxWorkers = 2
		entityType = "master"
	)
	ctx := context.Background()
	pg := testutils.GetDatabase(t, testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, pg))
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))
	sqlDB, err := db.DB()
	require.NoError(t, err)

	cache.ResetIDs()
	artistSeed := GetArtistStep(NewOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/artist.xml.gz",
		db,
	))()
	require.NoError(t, artistSeed.Err())

	dumps := []*model.DiscogsDump{
		importDump(entityType, "2026-07-01", "9"),
	}
	failedCoordinator := NewImportExecutionCoordinator(sqlDB, "resume-test")
	failedPreparation, err := failedCoordinator.Prepare(
		ctx,
		dumps,
		chunkSize,
		false,
		false,
	)
	require.NoError(t, err)
	installChunkFailure(t, db)

	failed := InsertMasterRelations(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		failedPreparation.RunID,
		entityType,
	))
	require.ErrorContains(t, failed.Err(), "intentional chunk failure")
	require.NoError(t, failedCoordinator.Complete(ctx, failed.Err()))

	committedBeforeRetry := completedChunkIndexes(t, db, failedPreparation.RunID, entityType)
	require.ElementsMatch(t, []int64{0, 2}, committedBeforeRetry)
	removeChunkFailure(t, db)

	retryCoordinator := NewImportExecutionCoordinator(sqlDB, "resume-test")
	retryPreparation, err := retryCoordinator.Prepare(
		ctx,
		dumps,
		chunkSize,
		false,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, failedPreparation.RunID, retryPreparation.ResumedFromRunID)
	require.ElementsMatch(
		t,
		committedBeforeRetry,
		completedChunkIndexes(t, db, retryPreparation.RunID, entityType),
	)
	require.Empty(t, completedChunkIndexes(t, db, failedPreparation.RunID, entityType))

	retried := InsertMasterRelations(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		retryPreparation.RunID,
		entityType,
	))
	require.NoError(t, retried.Err())
	require.NoError(t, retryCoordinator.Complete(ctx, nil))
	assertCompletedRunSummary(t, db, retryPreparation.RunID, entityType, 3, 3)
	require.Empty(t, completedChunkIndexes(t, db, retryPreparation.RunID, entityType))
	resumedState := normalizedBusinessState(t, db)

	forcedCoordinator := NewImportExecutionCoordinator(sqlDB, "resume-test")
	forcedPreparation, err := forcedCoordinator.Prepare(
		ctx,
		dumps,
		chunkSize,
		true,
		false,
	)
	require.NoError(t, err)
	require.Zero(t, forcedPreparation.ResumedFromRunID)
	forced := InsertMasterRelations(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		forcedPreparation.RunID,
		entityType,
	))
	require.NoError(t, forced.Err())
	require.NoError(t, forcedCoordinator.Complete(ctx, nil))
	require.Equal(t, resumedState, normalizedBusinessState(t, db))
}

func TestReleaseInterruptionConvergesWhenManifestExpands(t *testing.T) {
	const (
		chunkSize  = 1
		maxWorkers = 2
	)
	ctx := context.Background()
	pg := testutils.GetDatabase(t, testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, pg))
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))
	sqlDB, err := db.DB()
	require.NoError(t, err)

	cache.ResetIDs()
	for _, seed := range []struct {
		path string
		step func(Order) Step
	}{
		{path: "testdata/artist.xml.gz", step: newBatch().UpdateArtist},
		{path: "testdata/label.xml.gz", step: newBatch().UpdateLabel},
		{path: "testdata/master.xml.gz", step: newBatch().UpdateMaster},
	} {
		seedResult := seed.step(NewOrder(ctx, chunkSize, maxWorkers, seed.path, db))()
		require.NoError(t, seedResult.Err())
	}

	releaseDump := importDump("release", "2026-07-01", "8")
	interruptedCoordinator := NewImportExecutionCoordinator(sqlDB, "manifest-expansion-test")
	interruptedPreparation, err := interruptedCoordinator.Prepare(
		ctx,
		[]*model.DiscogsDump{releaseDump},
		chunkSize,
		false,
		false,
	)
	require.NoError(t, err)
	installChunkFailure(t, db)
	interrupted := insertReleases(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/release.xml.gz",
		db,
		interruptedPreparation.RunID,
		"release",
	))
	require.ErrorContains(t, interrupted.Err(), "intentional chunk failure")
	require.ElementsMatch(
		t,
		[]int64{0, 2},
		completedChunkIndexes(t, db, interruptedPreparation.RunID, "release"),
	)
	interruptedCoordinator.release(ctx)
	removeChunkFailure(t, db)

	expandedDumps := []*model.DiscogsDump{
		importDump("artist", "2026-07-01", "7"),
		releaseDump,
	}
	expandedCoordinator := NewImportExecutionCoordinator(sqlDB, "manifest-expansion-test")
	expandedPreparation, err := expandedCoordinator.Prepare(
		ctx,
		expandedDumps,
		chunkSize,
		false,
		false,
	)
	require.NoError(t, err)
	require.Zero(t, expandedPreparation.ResumedFromRunID)
	require.Empty(t, completedChunkIndexes(t, db, interruptedPreparation.RunID, "release"))
	var interruptedStatus string
	require.NoError(t, db.Raw(
		"select status from public.discogs_import_run where id = ?",
		interruptedPreparation.RunID,
	).Scan(&interruptedStatus).Error)
	require.Equal(t, "failed", interruptedStatus)

	cache.ResetIDs()
	config := koanf.New(".")
	require.NoError(t, config.Set("artists", true))
	require.NoError(t, config.Set("labels", false))
	require.NoError(t, config.Set("masters", false))
	require.NoError(t, config.Set("releases", true))
	require.NoError(t, preloadReferenceIDs(ctx, sqlDB, config))

	artistResult := GetArtistStep(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/artist.xml.gz",
		db,
		expandedPreparation.RunID,
		"artist",
	))()
	require.NoError(t, artistResult.Err())
	releaseResult := insertReleases(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/release.xml.gz",
		db,
		expandedPreparation.RunID,
		"release",
	))
	require.NoError(t, releaseResult.Err())
	require.NoError(t, expandedCoordinator.Complete(ctx, nil))
	assertCompletedRunSummary(t, db, expandedPreparation.RunID, "artist", 3, 3)
	assertCompletedRunSummary(t, db, expandedPreparation.RunID, "release", 3, 3)

	repeatedCoordinator := NewImportExecutionCoordinator(sqlDB, "manifest-expansion-test")
	repeatedPreparation, err := repeatedCoordinator.Prepare(
		ctx,
		expandedDumps,
		chunkSize,
		false,
		false,
	)
	require.NoError(t, err)
	require.True(t, repeatedPreparation.Skipped)
}

func installChunkFailure(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`
		create or replace function public.fail_selected_import_chunk()
		returns trigger
		language plpgsql
		as $function$
		begin
		    if new.chunk_index = 1 then
		        perform pg_sleep(0.25);
		        raise exception 'intentional chunk failure';
		    end if;
		    return new;
		end
		$function$;

		create trigger fail_selected_import_chunk_trigger
		before insert on public.discogs_import_run_chunk
		for each row execute function public.fail_selected_import_chunk();`).Error)
	t.Cleanup(func() {
		removeChunkFailure(t, db)
	})
}

func removeChunkFailure(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(
		"drop trigger if exists "+progressFailureTrigger+
			" on public.discogs_import_run_chunk",
	).Error)
	require.NoError(t, db.Exec(
		"drop function if exists public."+progressFailureFunction+"()",
	).Error)
}

func completedChunkIndexes(
	t *testing.T,
	db *gorm.DB,
	runID int64,
	entityType string,
) []int64 {
	t.Helper()
	indexes := make([]int64, 0)
	require.NoError(t, db.Raw(
		`select chunk_index
		   from public.discogs_import_run_chunk
		  where import_run_id = ?
		    and entity_type = ?
		  order by chunk_index`,
		runID,
		entityType,
	).Scan(&indexes).Error)
	return indexes
}

func assertCompletedRunSummary(
	t *testing.T,
	db *gorm.DB,
	runID int64,
	entityType string,
	wantItems int64,
	wantChunks int64,
) {
	t.Helper()
	type summary struct {
		ProcessedItems int64
		TotalItems     int64
		TotalChunks    int64
		Completed      bool
	}
	var actual summary
	require.NoError(t, db.Raw(
		`select processed_items,
		        total_items,
		        total_chunks,
		        completed_at is not null as completed
		   from public.discogs_import_run_dump
		  where import_run_id = ?
		    and entity_type = ?`,
		runID,
		entityType,
	).Scan(&actual).Error)
	require.Equal(t, wantItems, actual.ProcessedItems)
	require.Equal(t, wantItems, actual.TotalItems)
	require.Equal(t, wantChunks, actual.TotalChunks)
	require.True(t, actual.Completed)
}
