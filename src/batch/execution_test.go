package batch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestImportExecutionCoordinator(t *testing.T) {
	const testChunkSize = 5
	pg := testutils.GetDatabase(t, testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, pg))
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	ctx := context.Background()

	reset := func(t *testing.T) {
		t.Helper()
		require.NoError(t, db.Exec(`
			truncate table
			    public.discogs_import_run_dump,
			    public.discogs_import_run,
			    public.discogs_dump
			restart identity cascade`).Error)
	}
	complete := func(
		t *testing.T,
		prepared *ImportPreparation,
		dumps []*opendiscogsmodel.DiscogsDump,
	) {
		t.Helper()
		for _, dump := range dumps {
			order := NewTrackedOrder(
				ctx,
				testChunkSize,
				1,
				"unused",
				db,
				prepared.RunID,
				dump.EntityType,
			)
			require.NoError(t, completeEntityProgress(order, 0, 0))
		}
	}

	t.Run("skips a successful manifest unless forced", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "a"),
		}

		first := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := first.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		require.False(t, prepared.Skipped)
		complete(t, prepared, dumps)
		require.NoError(t, first.Complete(ctx, nil))

		repeated := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err = repeated.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		require.True(t, prepared.Skipped)

		forced := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err = forced.Prepare(ctx, dumps, testChunkSize, true, false)
		require.NoError(t, err)
		require.False(t, prepared.Skipped)
		complete(t, prepared, dumps)
		require.NoError(t, forced.Complete(ctx, nil))

		var forcedRuns int64
		require.NoError(t, db.Raw(`
			select count(*)
			  from public.discogs_import_run
			 where status = 'success'
			   and force_requested`).Scan(&forcedRuns).Error)
		require.Equal(t, int64(1), forcedRuns)
	})

	t.Run("allows disjoint entities and rejects overlap", func(t *testing.T) {
		reset(t)
		artistRelease := NewImportExecutionCoordinator(sqlDB, "test")
		artistReleaseDumps := []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "b"),
			importDump("release", "2026-07-01", "c"),
		}
		artistReleasePrepared, err := artistRelease.Prepare(
			ctx, artistReleaseDumps, testChunkSize, false, false,
		)
		require.NoError(t, err)

		master := NewImportExecutionCoordinator(sqlDB, "test")
		masterDumps := []*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-07-01", "d"),
		}
		masterPrepared, err := master.Prepare(ctx, masterDumps, testChunkSize, false, false)
		require.NoError(t, err)

		overlapping := NewImportExecutionCoordinator(sqlDB, "test")
		_, err = overlapping.Prepare(ctx, []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "e"),
		}, testChunkSize, false, false)
		require.ErrorContains(t, err, "already updating artist")

		complete(t, masterPrepared, masterDumps)
		require.NoError(t, master.Complete(ctx, nil))
		complete(t, artistReleasePrepared, artistReleaseDumps)
		require.NoError(t, artistRelease.Complete(ctx, nil))
	})

	t.Run("rejects downgrades unless separately authorized", func(t *testing.T) {
		reset(t)
		newer := NewImportExecutionCoordinator(sqlDB, "test")
		newerDumps := []*opendiscogsmodel.DiscogsDump{
			importDump("label", "2026-07-01", "f"),
		}
		newerPrepared, err := newer.Prepare(ctx, newerDumps, testChunkSize, false, false)
		require.NoError(t, err)
		complete(t, newerPrepared, newerDumps)
		require.NoError(t, newer.Complete(ctx, nil))

		olderDump := []*opendiscogsmodel.DiscogsDump{
			importDump("label", "2026-06-01", "1"),
		}
		older := NewImportExecutionCoordinator(sqlDB, "test")
		_, err = older.Prepare(ctx, olderDump, testChunkSize, true, false)
		require.ErrorContains(t, err, "predates checkpoint")

		authorized := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := authorized.Prepare(ctx, olderDump, testChunkSize, false, true)
		require.NoError(t, err)
		require.False(t, prepared.Skipped)
		complete(t, prepared, olderDump)
		require.NoError(t, authorized.Complete(ctx, nil))

		var allowed bool
		require.NoError(t, db.Raw(`
			select allow_downgrade_requested
			  from public.discogs_import_run
			 where id = ?`, prepared.RunID).Scan(&allowed).Error)
		require.True(t, allowed)
	})

	t.Run("retries a failed manifest", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-07-01", "2"),
		}
		failed := NewImportExecutionCoordinator(sqlDB, "test")
		_, err := failed.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		retry := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := retry.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		require.False(t, prepared.Skipped)
		require.NotZero(t, prepared.ResumedFromRunID)
		complete(t, prepared, dumps)
		require.NoError(t, retry.Complete(ctx, nil))
	})

	t.Run("marks an incomplete run failed", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "3"),
		}
		coordinator := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := coordinator.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)

		err = coordinator.Complete(ctx, nil)
		require.ErrorContains(t, err, "1 incomplete entities")

		var status string
		require.NoError(t, db.Raw(
			"select status from public.discogs_import_run where id = ?",
			prepared.RunID,
		).Scan(&status).Error)
		require.Equal(t, "failed", status)

		retry := NewImportExecutionCoordinator(sqlDB, "test")
		retried, err := retry.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		require.Equal(t, prepared.RunID, retried.ResumedFromRunID)
		require.NoError(t, retry.Complete(ctx, errors.New("fixture cleanup")))
	})

	t.Run("does not resume progress across processor versions", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("release", "2026-07-01", "4"),
		}
		failed := NewImportExecutionCoordinator(sqlDB, "old-version")
		failedPreparation, err := failed.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		retry := NewImportExecutionCoordinator(sqlDB, "new-version")
		prepared, err := retry.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		require.Zero(t, prepared.ResumedFromRunID)
		require.NotEqual(t, failedPreparation.RunID, prepared.RunID)
		require.NoError(t, retry.Complete(ctx, errors.New("fixture cleanup")))
	})

	t.Run("does not resume progress across chunk sizes", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-07-01", "5"),
		}
		failed := NewImportExecutionCoordinator(sqlDB, "test")
		failedPreparation, err := failed.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		retry := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := retry.Prepare(ctx, dumps, testChunkSize+1, false, false)
		require.NoError(t, err)
		require.Zero(t, prepared.ResumedFromRunID)
		require.NotEqual(t, failedPreparation.RunID, prepared.RunID)
		require.NoError(t, retry.Complete(ctx, errors.New("fixture cleanup")))
	})

	t.Run("rejects structurally invalid progress", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("label", "2026-07-01", "6"),
		}
		failed := NewImportExecutionCoordinator(sqlDB, "test")
		failedPreparation, err := failed.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(
				`insert into public.discogs_import_run_chunk
				    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
				 values (?, 'label', 0, 1, 1)`,
				failedPreparation.RunID,
			).Error; err != nil {
				return err
			}
			return tx.Exec(
				`update public.discogs_import_run_dump
				    set processed_items = 1,
				        last_progress_at = now()
				  where import_run_id = ?
				    and entity_type = 'label'`,
				failedPreparation.RunID,
			).Error
		}))
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		retry := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := retry.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		require.Zero(t, prepared.ResumedFromRunID)
		require.Empty(t, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"label",
		))
		require.NoError(t, retry.Complete(ctx, errors.New("fixture cleanup")))
	})
}

func importDump(entityType, date, checksumSeed string) *opendiscogsmodel.DiscogsDump {
	dumpDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		panic(err)
	}
	checksum := strings.Repeat(checksumSeed, 64)
	return &opendiscogsmodel.DiscogsDump{
		ETag:           fmt.Sprintf("%s-%s", entityType, date),
		DumpDate:       dumpDate,
		EntityType:     entityType,
		ChecksumSHA256: checksum,
		SizeBytes:      1,
		URI:            fmt.Sprintf("data/%s/%s.xml.gz", date, entityType),
	}
}
