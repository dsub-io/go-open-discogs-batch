package batch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	progressCompletionFailureFunction = "fail_import_completion"
	progressCompletionFailureTrigger  = "fail_import_completion_trigger"
	progressTransferFailureFunction   = "fail_import_progress_transfer"
	progressTransferFailureTrigger    = "fail_import_progress_transfer_trigger"
	intentionalChunkFailure           = "intentional chunk failure"
	chunkSynchronizationTimeout       = 10 * time.Second
)

func TestFailedRunRejectsLateChunkAndEntityCompletion(t *testing.T) {
	const (
		chunkSize  = 1
		entityType = "master"
	)
	ctx := context.Background()
	pg := testutils.GetDatabase(t, testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, pg))
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	dumps := []*model.DiscogsDump{
		importDump(entityType, "2026-07-01", "f"),
	}
	coordinator := NewImportExecutionCoordinator(sqlDB, "late-worker-test")
	preparation, err := coordinator.Prepare(ctx, dumps, chunkSize, false, false)
	require.NoError(t, err)
	require.NoError(t, coordinator.Complete(ctx, errors.New("fixture failure")))
	order := NewTrackedOrder(
		ctx,
		chunkSize,
		1,
		"unused",
		db,
		preparation.RunID,
		entityType,
		false,
	)

	lateChunk := executeChunk(
		order,
		ChunkMetadata{Index: 0, FirstItemIndex: 0, ItemCount: 1},
		func(transactionOrder Order) result.Result {
			writeErr := transactionOrder.getDB().Exec(
				"insert into public.genre (name) values ('late-worker-fixture')",
			).Error
			return result.NewResult(1, writeErr)
		},
	)
	require.ErrorContains(t, lateChunk.Err(), "run is not active")
	var genreRows int64
	require.NoError(t, db.Raw(
		"select count(*) from public.genre where name = 'late-worker-fixture'",
	).Scan(&genreRows).Error)
	require.Zero(t, genreRows)
	require.Empty(t, completedChunkIndexes(t, db, preparation.RunID, entityType))
	_, err = insertBySlice[*model.Genre](order)(ctx, []*model.Genre{{Name: "late-simple-fixture"}})
	require.ErrorContains(t, err, "run is not active")
	require.NoError(t, db.Raw(
		"select count(*) from public.genre where name = 'late-simple-fixture'",
	).Scan(&genreRows).Error)
	require.Zero(t, genreRows)

	err = completeEntityProgress(order, 0, 0)
	require.ErrorContains(t, err, "chunk coverage does not match")
	var completed bool
	require.NoError(t, db.Raw(
		`select completed_at is not null
		   from public.discogs_import_run_dump
		  where import_run_id = ?
		    and entity_type = ?`,
		preparation.RunID,
		entityType,
	).Scan(&completed).Error)
	require.False(t, completed)
}

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
	failedOrder := NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		failedPreparation.RunID,
		entityType,
		false,
	)
	failed := processRelationChunks(
		failedOrder,
		"master relations",
		"master",
		"source-read master relations",
		failChunkAfterLaterChunkCompletes(
			func(order Order, chunk ChunkMetadata, items []*XmlMasterRelation) result.Result {
				return writeMasterRelationChunk(order, chunk, items, false)
			},
		),
	)
	require.ErrorContains(t, failed.Err(), "intentional chunk failure")
	require.NoError(t, failedCoordinator.Complete(ctx, failed.Err()))

	committedBeforeRetry := completedChunkIndexes(t, db, failedPreparation.RunID, entityType)
	require.ElementsMatch(t, []int64{0, 2}, committedBeforeRetry)
	var rolledBackRows int64
	require.NoError(t, db.Raw(
		"select count(*) from public.master where id = 2",
	).Scan(&rolledBackRows).Error)
	require.Zero(t, rolledBackRows)

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
		true,
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
		false,
	))
	require.NoError(t, forced.Err())
	require.NoError(t, forcedCoordinator.Complete(ctx, nil))
	require.Equal(t, resumedState, normalizedBusinessState(t, db))
}

func TestImportCompletionFailureRemainsResumable(t *testing.T) {
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
		importDump(entityType, "2026-07-01", "a"),
	}
	coordinator := NewImportExecutionCoordinator(sqlDB, "completion-failure-test")
	preparation, err := coordinator.Prepare(ctx, dumps, chunkSize, false, false)
	require.NoError(t, err)
	written := InsertMasterRelations(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		preparation.RunID,
		entityType,
		false,
	))
	require.NoError(t, written.Err())
	installCompletionFailure(t, db)

	err = coordinator.Complete(ctx, nil)
	require.ErrorContains(t, err, "intentional completion failure")
	var status string
	require.NoError(t, db.Raw(
		"select status from public.discogs_import_run where id = ?",
		preparation.RunID,
	).Scan(&status).Error)
	require.Equal(t, "running", status)
	require.ElementsMatch(
		t,
		[]int64{0, 1, 2},
		completedChunkIndexes(t, db, preparation.RunID, entityType),
	)
	removeCompletionFailure(t, db)

	retry := NewImportExecutionCoordinator(sqlDB, "completion-failure-test")
	retryPreparation, err := retry.Prepare(ctx, dumps, chunkSize, false, false)
	require.NoError(t, err)
	require.Equal(t, preparation.RunID, retryPreparation.ResumedFromRunID)
	retried := InsertMasterRelations(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		retryPreparation.RunID,
		entityType,
		true,
	))
	require.NoError(t, retried.Err())
	require.Zero(t, retried.Count())
	require.NoError(t, retry.Complete(ctx, nil))
}

func TestResumeAdmissionTransferIsAtomic(t *testing.T) {
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
		importDump(entityType, "2026-07-01", "b"),
	}
	failed := NewImportExecutionCoordinator(sqlDB, "transfer-failure-test")
	failedPreparation, err := failed.Prepare(ctx, dumps, chunkSize, false, false)
	require.NoError(t, err)
	written := InsertMasterRelations(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		failedPreparation.RunID,
		entityType,
		false,
	))
	require.NoError(t, written.Err())
	require.NoError(t, failed.Complete(ctx, context.Canceled))
	installTransferFailure(t, db)

	failedTransfer := NewImportExecutionCoordinator(sqlDB, "transfer-failure-test")
	_, err = failedTransfer.Prepare(ctx, dumps, chunkSize, false, false)
	require.ErrorContains(t, err, "intentional progress transfer failure")
	require.ElementsMatch(
		t,
		[]int64{0, 1, 2},
		completedChunkIndexes(t, db, failedPreparation.RunID, entityType),
	)
	var runCount int64
	require.NoError(t, db.Raw(
		"select count(*) from public.discogs_import_run",
	).Scan(&runCount).Error)
	require.Equal(t, int64(1), runCount)
	removeTransferFailure(t, db)

	retry := NewImportExecutionCoordinator(sqlDB, "transfer-failure-test")
	retryPreparation, err := retry.Prepare(ctx, dumps, chunkSize, false, false)
	require.NoError(t, err)
	require.Equal(t, failedPreparation.RunID, retryPreparation.ResumedFromRunID)
	retried := InsertMasterRelations(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		retryPreparation.RunID,
		entityType,
		true,
	))
	require.NoError(t, retried.Err())
	require.Zero(t, retried.Count())
	require.NoError(t, retry.Complete(ctx, nil))
}

func TestMultiEntityResumeSkipsCompletedEntity(t *testing.T) {
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
	dumps := []*model.DiscogsDump{
		importDump("artist", "2026-07-01", "d"),
		importDump("master", "2026-07-01", "e"),
	}

	cache.ResetIDs()
	failed := NewImportExecutionCoordinator(sqlDB, "multi-entity-resume-test")
	failedPreparation, err := failed.Prepare(ctx, dumps, chunkSize, false, false)
	require.NoError(t, err)
	artist := GetArtistStep(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/artist.xml.gz",
		db,
		failedPreparation.RunID,
		"artist",
		false,
	))()
	require.NoError(t, artist.Err())
	assertCompletedRunSummary(t, db, failedPreparation.RunID, "artist", 3, 3)
	require.NoError(t, failed.Complete(ctx, context.Canceled))

	cache.ResetIDs()
	retry := NewImportExecutionCoordinator(sqlDB, "multi-entity-resume-test")
	retryPreparation, err := retry.Prepare(ctx, dumps, chunkSize, false, false)
	require.NoError(t, err)
	require.Equal(t, failedPreparation.RunID, retryPreparation.ResumedFromRunID)
	repeatedArtist := GetArtistStep(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/artist.xml.gz",
		db,
		retryPreparation.RunID,
		"artist",
		true,
	))()
	require.NoError(t, repeatedArtist.Err())
	require.Zero(t, repeatedArtist.Count())
	master := InsertMasterRelations(NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/master.xml.gz",
		db,
		retryPreparation.RunID,
		"master",
		true,
	))
	require.NoError(t, master.Err())
	require.NoError(t, retry.Complete(ctx, nil))
	assertCompletedRunSummary(t, db, retryPreparation.RunID, "artist", 3, 3)
	assertCompletedRunSummary(t, db, retryPreparation.RunID, "master", 3, 3)
	require.Empty(t, completedChunkIndexes(t, db, retryPreparation.RunID, "artist"))
	require.Empty(t, completedChunkIndexes(t, db, retryPreparation.RunID, "master"))
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
	interruptedOrder := NewTrackedOrder(
		ctx,
		chunkSize,
		maxWorkers,
		"testdata/release.xml.gz",
		db,
		interruptedPreparation.RunID,
		"release",
		false,
	)
	interrupted := processRelationChunks(
		interruptedOrder,
		"release relations",
		"release",
		"source-read release relations",
		failChunkAfterLaterChunkCompletes(
			func(order Order, chunk ChunkMetadata, items []*XmlReleaseRelation) result.Result {
				return writeReleaseRelationChunk(order, chunk, items, false)
			},
		),
	)
	require.ErrorContains(t, interrupted.Err(), "intentional chunk failure")
	require.ElementsMatch(
		t,
		[]int64{0, 2},
		completedChunkIndexes(t, db, interruptedPreparation.RunID, "release"),
	)
	interruptedCoordinator.release(ctx)

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
	require.ElementsMatch(
		t,
		[]int64{0, 2},
		completedChunkIndexes(t, db, interruptedPreparation.RunID, "release"),
	)
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
		false,
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
		false,
	))
	require.NoError(t, releaseResult.Err())
	require.NoError(t, expandedCoordinator.Complete(ctx, nil))
	assertCompletedRunSummary(t, db, expandedPreparation.RunID, "artist", 3, 3)
	assertCompletedRunSummary(t, db, expandedPreparation.RunID, "release", 3, 3)
	require.Empty(t, completedChunkIndexes(t, db, interruptedPreparation.RunID, "release"))

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

func failChunkAfterLaterChunkCompletes[T any](
	write relationChunkWriter[T],
) relationChunkWriter[T] {
	const (
		failedChunkIndex = int64(1)
		laterChunkIndex  = int64(2)
	)
	laterChunkCompleted := make(chan struct{})
	return func(order Order, chunk ChunkMetadata, items []*T) result.Result {
		switch chunk.Index {
		case failedChunkIndex:
			timer := time.NewTimer(chunkSynchronizationTimeout)
			defer timer.Stop()
			select {
			case <-laterChunkCompleted:
				return result.NewResult(0, errors.New(intentionalChunkFailure))
			case <-timer.C:
				return result.NewResult(
					0,
					errors.New("timed out waiting for the later chunk to complete"),
				)
			}
		case laterChunkIndex:
			written := write(order, chunk, items)
			close(laterChunkCompleted)
			return written
		default:
			return write(order, chunk, items)
		}
	}
}

func installCompletionFailure(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`
		create or replace function public.fail_import_completion()
		returns trigger
		language plpgsql
		as $function$
		begin
		    if new.status = 'success' then
		        raise exception 'intentional completion failure';
		    end if;
		    return new;
		end
		$function$;

		create trigger fail_import_completion_trigger
		before update on public.discogs_import_run
		for each row execute function public.fail_import_completion();`).Error)
	t.Cleanup(func() {
		removeCompletionFailure(t, db)
	})
}

func removeCompletionFailure(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(
		"drop trigger if exists "+progressCompletionFailureTrigger+
			" on public.discogs_import_run",
	).Error)
	require.NoError(t, db.Exec(
		"drop function if exists public."+progressCompletionFailureFunction+"()",
	).Error)
}

func installTransferFailure(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`
		create or replace function public.fail_import_progress_transfer()
		returns trigger
		language plpgsql
		as $function$
		begin
		    raise exception 'intentional progress transfer failure';
		end
		$function$;

		create trigger fail_import_progress_transfer_trigger
		before delete on public.discogs_import_run_chunk
		for each row execute function public.fail_import_progress_transfer();`).Error)
	t.Cleanup(func() {
		removeTransferFailure(t, db)
	})
}

func removeTransferFailure(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(
		"drop trigger if exists "+progressTransferFailureTrigger+
			" on public.discogs_import_run_chunk",
	).Error)
	require.NoError(t, db.Exec(
		"drop function if exists public."+progressTransferFailureFunction+"()",
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
