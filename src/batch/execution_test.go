package batch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/data"
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
				false,
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

	t.Run("reapplies a historical manifest when the current checkpoint changed", func(t *testing.T) {
		reset(t)
		julyDump := importDump("artist", "2026-07-01", "b")
		augustDump := importDump("artist", "2026-08-01", "c")

		july := NewImportExecutionCoordinator(sqlDB, "test")
		julyPreparation, err := july.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{julyDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, julyPreparation, []*opendiscogsmodel.DiscogsDump{julyDump})
		require.NoError(t, july.Complete(ctx, nil))

		august := NewImportExecutionCoordinator(sqlDB, "test")
		augustPreparation, err := august.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{augustDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, augustPreparation, []*opendiscogsmodel.DiscogsDump{augustDump})
		require.NoError(t, august.Complete(ctx, nil))

		reapplyJuly := NewImportExecutionCoordinator(sqlDB, "test")
		reappliedJuly, err := reapplyJuly.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{julyDump},
			testChunkSize,
			false,
			true,
		)
		require.NoError(t, err)
		require.False(t, reappliedJuly.Skipped)
		complete(t, reappliedJuly, []*opendiscogsmodel.DiscogsDump{julyDump})
		require.NoError(t, reapplyJuly.Complete(ctx, nil))

		reapplyAugust := NewImportExecutionCoordinator(sqlDB, "test")
		reappliedAugust, err := reapplyAugust.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{augustDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.False(t, reappliedAugust.Skipped)
		complete(t, reappliedAugust, []*opendiscogsmodel.DiscogsDump{augustDump})
		require.NoError(t, reapplyAugust.Complete(ctx, nil))

		currentAugust := NewImportExecutionCoordinator(sqlDB, "test")
		currentPreparation, err := currentAugust.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{augustDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.True(t, currentPreparation.Skipped)
	})

	t.Run("does not skip a checkpoint dirtied by a later failed run", func(t *testing.T) {
		reset(t)
		julyDump := importDump("master", "2026-07-01", "d")
		july := NewImportExecutionCoordinator(sqlDB, "test")
		julyPreparation, err := july.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{julyDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, julyPreparation, []*opendiscogsmodel.DiscogsDump{julyDump})
		require.NoError(t, july.Complete(ctx, nil))

		augustDump := importDump("master", "2026-08-01", "e")
		failedAugust := NewImportExecutionCoordinator(sqlDB, "test")
		failedPreparation, err := failedAugust.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{augustDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
			if insertErr := tx.Exec(
				`insert into public.discogs_import_run_chunk
				    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
				 values (?, 'master', 0, 0, 1)`,
				failedPreparation.RunID,
			).Error; insertErr != nil {
				return insertErr
			}
			return tx.Exec(
				`update public.discogs_import_run_dump
				    set processed_items = 1,
				        last_progress_at = now()
				  where import_run_id = ?
				    and entity_type = 'master'`,
				failedPreparation.RunID,
			).Error
		}))
		require.NoError(t, failedAugust.Complete(ctx, errors.New("fixture failure")))

		repairJuly := NewImportExecutionCoordinator(sqlDB, "test")
		repairPreparation, err := repairJuly.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{julyDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.False(t, repairPreparation.Skipped)
		complete(t, repairPreparation, []*opendiscogsmodel.DiscogsDump{julyDump})
		require.NoError(t, repairJuly.Complete(ctx, nil))
	})

	t.Run("locks reference dependencies while allowing independent entities", func(t *testing.T) {
		reset(t)
		artist := NewImportExecutionCoordinator(sqlDB, "test")
		artistDumps := []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "b"),
		}
		artistPrepared, err := artist.Prepare(
			ctx, artistDumps, testChunkSize, false, false,
		)
		require.NoError(t, err)

		label := NewImportExecutionCoordinator(sqlDB, "test")
		labelDumps := []*opendiscogsmodel.DiscogsDump{
			importDump("label", "2026-07-01", "c"),
		}
		labelPrepared, err := label.Prepare(ctx, labelDumps, testChunkSize, false, false)
		require.NoError(t, err)

		master := NewImportExecutionCoordinator(sqlDB, "test")
		masterDumps := []*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-07-01", "d"),
		}
		_, err = master.Prepare(ctx, masterDumps, testChunkSize, false, false)
		require.ErrorContains(t, err, "already updating artist")

		release := NewImportExecutionCoordinator(sqlDB, "test")
		_, err = release.Prepare(ctx, []*opendiscogsmodel.DiscogsDump{
			importDump("release", "2026-07-01", "e"),
		}, testChunkSize, false, false)
		require.ErrorContains(t, err, "already updating artist")

		complete(t, labelPrepared, labelDumps)
		require.NoError(t, label.Complete(ctx, nil))
		complete(t, artistPrepared, artistDumps)
		require.NoError(t, artist.Complete(ctx, nil))
	})

	t.Run("release locks every referenced entity and master write target", func(t *testing.T) {
		reset(t)
		release := NewImportExecutionCoordinator(sqlDB, "test")
		releaseDumps := []*opendiscogsmodel.DiscogsDump{
			importDump("release", "2026-07-01", "e"),
		}
		releasePrepared, err := release.Prepare(
			ctx, releaseDumps, testChunkSize, false, false,
		)
		require.NoError(t, err)

		for entityType, blockedLock := range map[string]string{
			"artist":  "artist",
			"label":   "label",
			"master":  "artist",
			"release": "artist",
		} {
			competing := NewImportExecutionCoordinator(sqlDB, "test")
			_, err = competing.Prepare(ctx, []*opendiscogsmodel.DiscogsDump{
				importDump(entityType, "2026-07-01", "f"),
			}, testChunkSize, false, false)
			require.ErrorContains(t, err, "already updating "+blockedLock)
		}

		complete(t, releasePrepared, releaseDumps)
		require.NoError(t, release.Complete(ctx, nil))
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

	t.Run("does not resume progress overwritten by a newer successful dump", func(t *testing.T) {
		reset(t)
		julyDump := importDump("master", "2026-07-01", "6")
		failed := NewImportExecutionCoordinator(sqlDB, "test")
		failedPreparation, err := failed.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{julyDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
			if insertErr := tx.Exec(
				`insert into public.discogs_import_run_chunk
				    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
				 values (?, 'master', 0, 0, 1)`,
				failedPreparation.RunID,
			).Error; insertErr != nil {
				return insertErr
			}
			return tx.Exec(
				`update public.discogs_import_run_dump
				    set processed_items = 1,
				        last_progress_at = now()
				  where import_run_id = ?
				    and entity_type = 'master'`,
				failedPreparation.RunID,
			).Error
		}))
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		augustDump := importDump("master", "2026-08-01", "7")
		august := NewImportExecutionCoordinator(sqlDB, "test")
		augustPreparation, err := august.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{augustDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, augustPreparation, []*opendiscogsmodel.DiscogsDump{augustDump})
		require.NoError(t, august.Complete(ctx, nil))

		retryJuly := NewImportExecutionCoordinator(sqlDB, "test")
		retryPreparation, err := retryJuly.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{julyDump},
			testChunkSize,
			false,
			true,
		)
		require.NoError(t, err)
		require.Zero(t, retryPreparation.ResumedFromRunID)
		require.ElementsMatch(t, []int64{0}, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"master",
		))
		require.NoError(t, retryJuly.Complete(ctx, errors.New("fixture cleanup")))
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
		require.ElementsMatch(t, []int64{0}, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"label",
		))
		complete(t, prepared, dumps)
		require.NoError(t, retry.Complete(ctx, nil))
		require.Empty(t, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"label",
		))
	})

	t.Run("preserves a failed multi-entity ledger until every dump is superseded", func(t *testing.T) {
		reset(t)
		artistDump := importDump("artist", "2026-07-01", "7")
		releaseDump := importDump("release", "2026-07-01", "8")
		failed := NewImportExecutionCoordinator(sqlDB, "test")
		failedPreparation, err := failed.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{artistDump, releaseDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		for _, entityType := range []string{"artist", "release"} {
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
				if err := tx.Exec(
					`insert into public.discogs_import_run_chunk
					    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
					 values (?, ?, 0, 0, 1)`,
					failedPreparation.RunID,
					entityType,
				).Error; err != nil {
					return err
				}
				return tx.Exec(
					`update public.discogs_import_run_dump
					    set processed_items = 1,
					        last_progress_at = now()
					  where import_run_id = ?
					    and entity_type = ?`,
					failedPreparation.RunID,
					entityType,
				).Error
			}))
		}
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		artist := NewImportExecutionCoordinator(sqlDB, "test")
		artistPreparation, err := artist.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{artistDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.Zero(t, artistPreparation.ResumedFromRunID)
		require.ElementsMatch(t, []int64{0}, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"artist",
		))
		require.ElementsMatch(t, []int64{0}, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"release",
		))
		complete(t, artistPreparation, []*opendiscogsmodel.DiscogsDump{artistDump})
		require.NoError(t, artist.Complete(ctx, nil))
		require.ElementsMatch(t, []int64{0}, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"artist",
		))
		require.ElementsMatch(t, []int64{0}, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"release",
		))

		release := NewImportExecutionCoordinator(sqlDB, "test")
		releasePreparation, err := release.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{releaseDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.Zero(t, releasePreparation.ResumedFromRunID)
		complete(t, releasePreparation, []*opendiscogsmodel.DiscogsDump{releaseDump})
		require.NoError(t, release.Complete(ctx, nil))
		require.Empty(t, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"artist",
		))
		require.Empty(t, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"release",
		))
	})

	t.Run("does not prune progress superseded by another processor version", func(t *testing.T) {
		reset(t)
		artistDump := importDump("artist", "2026-07-01", "a")
		releaseDump := importDump("release", "2026-07-01", "b")
		for _, dump := range []*opendiscogsmodel.DiscogsDump{artistDump, releaseDump} {
			oldProcessor := NewImportExecutionCoordinator(sqlDB, "old-version")
			prepared, prepareErr := oldProcessor.Prepare(
				ctx,
				[]*opendiscogsmodel.DiscogsDump{dump},
				testChunkSize,
				false,
				false,
			)
			require.NoError(t, prepareErr)
			complete(t, prepared, []*opendiscogsmodel.DiscogsDump{dump})
			require.NoError(t, oldProcessor.Complete(ctx, nil))
		}

		failed := NewImportExecutionCoordinator(sqlDB, "new-version")
		failedPreparation, err := failed.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{artistDump, releaseDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		for _, entityType := range []string{"artist", "release"} {
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
				if insertErr := tx.Exec(
					`insert into public.discogs_import_run_chunk
					    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
					 values (?, ?, 0, 0, 1)`,
					failedPreparation.RunID,
					entityType,
				).Error; insertErr != nil {
					return insertErr
				}
				return tx.Exec(
					`update public.discogs_import_run_dump
					    set processed_items = 1,
					        last_progress_at = now()
					  where import_run_id = ?
					    and entity_type = ?`,
					failedPreparation.RunID,
					entityType,
				).Error
			}))
		}
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		masterDump := importDump("master", "2026-07-01", "c")
		master := NewImportExecutionCoordinator(sqlDB, "new-version")
		masterPreparation, err := master.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{masterDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, masterPreparation, []*opendiscogsmodel.DiscogsDump{masterDump})
		require.NoError(t, master.Complete(ctx, nil))

		for _, entityType := range []string{"artist", "release"} {
			require.ElementsMatch(t, []int64{0}, completedChunkIndexes(
				t,
				db,
				failedPreparation.RunID,
				entityType,
			))
		}
	})

	t.Run("does not prune failed progress based on a historical checkpoint", func(t *testing.T) {
		reset(t)
		julyDump := importDump("artist", "2026-07-01", "d")
		augustDump := importDump("artist", "2026-08-01", "e")
		for _, dump := range []*opendiscogsmodel.DiscogsDump{julyDump, augustDump} {
			coordinator := NewImportExecutionCoordinator(sqlDB, "test")
			prepared, prepareErr := coordinator.Prepare(
				ctx,
				[]*opendiscogsmodel.DiscogsDump{dump},
				testChunkSize,
				false,
				false,
			)
			require.NoError(t, prepareErr)
			complete(t, prepared, []*opendiscogsmodel.DiscogsDump{dump})
			require.NoError(t, coordinator.Complete(ctx, nil))
		}

		failedJuly := NewImportExecutionCoordinator(sqlDB, "test")
		failedPreparation, err := failedJuly.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{julyDump},
			testChunkSize,
			false,
			true,
		)
		require.NoError(t, err)
		require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
			if insertErr := tx.Exec(
				`insert into public.discogs_import_run_chunk
				    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
				 values (?, 'artist', 0, 0, 1)`,
				failedPreparation.RunID,
			).Error; insertErr != nil {
				return insertErr
			}
			return tx.Exec(
				`update public.discogs_import_run_dump
				    set processed_items = 1,
				        last_progress_at = now()
				  where import_run_id = ?
				    and entity_type = 'artist'`,
				failedPreparation.RunID,
			).Error
		}))
		require.NoError(t, failedJuly.Complete(ctx, errors.New("fixture failure")))

		masterDump := importDump("master", "2026-08-01", "f")
		master := NewImportExecutionCoordinator(sqlDB, "test")
		masterPreparation, err := master.Prepare(
			ctx,
			[]*opendiscogsmodel.DiscogsDump{masterDump},
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, masterPreparation, []*opendiscogsmodel.DiscogsDump{masterDump})
		require.NoError(t, master.Complete(ctx, nil))
		require.ElementsMatch(t, []int64{0}, completedChunkIndexes(
			t,
			db,
			failedPreparation.RunID,
			"artist",
		))
	})

	t.Run("rejects an out-of-range chunk even when counts match", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-07-01", "9"),
		}
		coordinator := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := coordinator.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		for _, chunkIndex := range []int64{0, 1, 3} {
			require.NoError(t, db.Exec(
				`insert into public.discogs_import_run_chunk
				    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
				 values (?, 'master', ?, ?, ?)`,
				prepared.RunID,
				chunkIndex,
				chunkIndex*testChunkSize,
				testChunkSize,
			).Error)
		}
		require.NoError(t, db.Exec(
			`update public.discogs_import_run_dump
			    set processed_items = 15,
			        last_progress_at = now()
			  where import_run_id = ?
			    and entity_type = 'master'`,
			prepared.RunID,
		).Error)
		order := NewTrackedOrder(
			ctx,
			testChunkSize,
			1,
			"unused",
			db,
			prepared.RunID,
			"master",
			false,
		)

		err = completeEntityProgress(order, 15, 3)
		require.ErrorContains(t, err, "chunk coverage does not match")
		require.NoError(t, coordinator.Complete(ctx, errors.New("fixture cleanup")))
	})

	t.Run("keeps success durable when cleanup fails", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "a"),
		}
		coordinator := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := coordinator.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		complete(t, prepared, dumps)
		nonEmptyDirectory := filepath.Join(t.TempDir(), "retained-download")
		require.NoError(t, os.Mkdir(nonEmptyDirectory, 0755))
		require.NoError(t, os.WriteFile(
			filepath.Join(nonEmptyDirectory, "data"),
			[]byte("fixture"),
			0644,
		))
		plan := &data.ImportPlan{
			Resources: map[string]string{"artists": nonEmptyDirectory},
			Dumps:     dumps,
		}

		err = finalizeImport(ctx, coordinator, plan, true, nil)
		require.Error(t, err)
		var status string
		require.NoError(t, db.Raw(
			"select status from public.discogs_import_run where id = ?",
			prepared.RunID,
		).Scan(&status).Error)
		require.Equal(t, "success", status)

		repeated := NewImportExecutionCoordinator(sqlDB, "test")
		repeatedPreparation, err := repeated.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.True(t, repeatedPreparation.Skipped)
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
