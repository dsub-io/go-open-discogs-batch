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
	installImportContractRevisionFixture(t, db)
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
		seedPendingCatalogStates(t, db)
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

	t.Run("reprocesses successful runs from an older contract revision", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump(releaseEntityType, "2026-07-01", "0"),
		}

		legacy := NewImportExecutionCoordinator(sqlDB, "old-go-version")
		legacyPreparation, err := legacy.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, legacyPreparation, dumps)
		require.NoError(t, legacy.Complete(ctx, nil))
		setImportContractRevision(
			t,
			db,
			legacyPreparation.RunID,
			legacyImportContractRevision,
		)

		current := NewImportExecutionCoordinator(sqlDB, "new-go-version")
		currentPreparation, err := current.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.False(t, currentPreparation.Skipped)
		require.Zero(t, currentPreparation.ResumedFromRunID)
		requireImportContractRevision(
			t,
			db,
			currentPreparation.RunID,
			currentImportContractRevisions[releaseEntityType],
		)
		complete(t, currentPreparation, dumps)
		require.NoError(t, current.Complete(ctx, nil))

		repeated := NewImportExecutionCoordinator(sqlDB, "another-go-version")
		repeatedPreparation, err := repeated.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.True(t, repeatedPreparation.Skipped)
		require.Equal(t, currentPreparation.RunID, repeatedPreparation.RunID)
	})

	t.Run("narrows a mixed outdated manifest to release and preserves valid checkpoints", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump(artistEntityType, "2026-07-01", "1"),
			importDump(labelEntityType, "2026-07-01", "2"),
			importDump(masterEntityType, "2026-07-01", "3"),
			importDump(releaseEntityType, "2026-07-01", "4"),
		}
		legacy := NewImportExecutionCoordinator(sqlDB, "legacy")
		legacyPreparation, err := legacy.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, legacyPreparation, dumps)
		require.NoError(t, legacy.Complete(ctx, nil))
		setImportContractRevision(
			t,
			db,
			legacyPreparation.RunID,
			legacyImportContractRevision,
		)
		checkpointRunIDs := importCheckpointRunIDs(t, db)

		mixed := NewImportExecutionCoordinator(sqlDB, "current")
		_, err = mixed.Prepare(ctx, dumps, testChunkSize, false, false)
		require.ErrorContains(t, err, "rerun only --entities release")
		require.Equal(t, checkpointRunIDs, importCheckpointRunIDs(t, db))

		releaseOnly := NewImportExecutionCoordinator(sqlDB, "current")
		releasePreparation, err := releaseOnly.Prepare(
			ctx,
			dumps[3:],
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.False(t, releasePreparation.Skipped)
		requireImportContractRevision(
			t,
			db,
			releasePreparation.RunID,
			currentImportContractRevisions[releaseEntityType],
		)
		complete(t, releasePreparation, dumps[3:])
		require.NoError(t, releaseOnly.Complete(ctx, nil))

		currentCheckpointRunIDs := importCheckpointRunIDs(t, db)
		for _, entityType := range []string{
			artistEntityType,
			labelEntityType,
			masterEntityType,
		} {
			require.Equal(
				t,
				checkpointRunIDs[entityType],
				currentCheckpointRunIDs[entityType],
			)
		}
		require.Equal(
			t,
			releasePreparation.RunID,
			currentCheckpointRunIDs[releaseEntityType],
		)

		consolidated := NewImportExecutionCoordinator(sqlDB, "current")
		consolidatedPreparation, err := consolidated.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.True(t, consolidatedPreparation.Skipped)
		require.NotEqual(t, legacyPreparation.RunID, consolidatedPreparation.RunID)
		requireRunDumpContractRevisions(
			t,
			db,
			consolidatedPreparation.RunID,
			currentImportContractRevisions,
		)
	})

	t.Run("shares successful current-revision runs across processors", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("label", "2026-07-01", "0"),
		}
		javaRun := NewImportExecutionCoordinator(sqlDB, "go-fixture")
		javaPreparation, err := javaRun.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, javaPreparation, dumps)
		require.NoError(t, javaRun.Complete(ctx, nil))
		require.NoError(t, db.Exec(
			`update public.discogs_import_run
			    set processor = ?, processor_version = ?
			  where id = ?`,
			"open-discogs-batch",
			"java-fixture",
			javaPreparation.RunID,
		).Error)

		goRun := NewImportExecutionCoordinator(sqlDB, "go-fixture")
		goPreparation, err := goRun.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.True(t, goPreparation.Skipped)
		require.Equal(t, javaPreparation.RunID, goPreparation.RunID)
	})

	t.Run("dirties current success after a failed older-revision attempt", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("release", "2026-07-01", "0"),
		}
		successful := NewImportExecutionCoordinator(sqlDB, "test")
		successfulPreparation, err := successful.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, successfulPreparation, dumps)
		require.NoError(t, successful.Complete(ctx, nil))

		legacyFailure := NewImportExecutionCoordinator(sqlDB, "test")
		legacyFailurePreparation, err := legacyFailure.Prepare(
			ctx,
			dumps,
			testChunkSize,
			true,
			false,
		)
		require.NoError(t, err)
		setImportContractRevision(
			t,
			db,
			legacyFailurePreparation.RunID,
			legacyImportContractRevision,
		)
		require.NoError(t, legacyFailure.Complete(ctx, errors.New("fixture failure")))

		repeated := NewImportExecutionCoordinator(sqlDB, "test")
		repeatedPreparation, err := repeated.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.False(t, repeatedPreparation.Skipped)
		require.NotEqual(t, successfulPreparation.RunID, repeatedPreparation.RunID)
		require.NoError(t, repeated.Complete(ctx, errors.New("fixture cleanup")))
	})

	t.Run("resumes abandoned and failed runs only at the current revision", func(t *testing.T) {
		for _, test := range []struct {
			name       string
			revision   importContractRevision
			abandoned  bool
			wantResume bool
		}{
			{
				name:       "current failed",
				revision:   currentImportContractRevisions[masterEntityType],
				wantResume: true,
			},
			{
				name:      "incompatible failed",
				revision:  incompatibleImportContractRevision,
				abandoned: false,
			},
			{
				name:       "current abandoned",
				revision:   currentImportContractRevisions[masterEntityType],
				abandoned:  true,
				wantResume: true,
			},
			{
				name:      "incompatible abandoned",
				revision:  incompatibleImportContractRevision,
				abandoned: true,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				reset(t)
				dumps := []*opendiscogsmodel.DiscogsDump{
					importDump("master", "2026-07-01", "0"),
				}
				previous := NewImportExecutionCoordinator(sqlDB, "same-version")
				previousPreparation, err := previous.Prepare(
					ctx,
					dumps,
					testChunkSize,
					false,
					false,
				)
				require.NoError(t, err)
				setImportContractRevision(
					t,
					db,
					previousPreparation.RunID,
					test.revision,
				)
				if test.abandoned {
					previous.release(ctx)
				} else {
					require.NoError(t, previous.Complete(ctx, errors.New("fixture failure")))
				}

				retry := NewImportExecutionCoordinator(sqlDB, "same-version")
				retryPreparation, err := retry.Prepare(
					ctx,
					dumps,
					testChunkSize,
					false,
					false,
				)
				require.NoError(t, err)
				if test.wantResume {
					require.Equal(
						t,
						previousPreparation.RunID,
						retryPreparation.ResumedFromRunID,
					)
				} else {
					require.Zero(t, retryPreparation.ResumedFromRunID)
				}
				requireImportContractRevision(
					t,
					db,
					retryPreparation.RunID,
					currentImportContractRevisions[masterEntityType],
				)
				require.NoError(t, retry.Complete(ctx, errors.New("fixture cleanup")))

				var previousStatus string
				require.NoError(t, db.Raw(
					"select status from public.discogs_import_run where id = ?",
					previousPreparation.RunID,
				).Scan(&previousStatus).Error)
				require.Equal(t, "failed", previousStatus)
			})
		}
	})

	t.Run("does not resume across a newer checkpoint from another revision", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-07-01", "0"),
		}
		failed := NewImportExecutionCoordinator(sqlDB, "same-version")
		failedPreparation, err := failed.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		legacyCheckpoint := NewImportExecutionCoordinator(sqlDB, "same-version")
		legacyCheckpointPreparation, err := legacyCheckpoint.Prepare(
			ctx,
			dumps,
			testChunkSize,
			true,
			false,
		)
		require.NoError(t, err)
		complete(t, legacyCheckpointPreparation, dumps)
		require.NoError(t, legacyCheckpoint.Complete(ctx, nil))
		setImportContractRevision(
			t,
			db,
			legacyCheckpointPreparation.RunID,
			incompatibleImportContractRevision,
		)

		retry := NewImportExecutionCoordinator(sqlDB, "same-version")
		retryPreparation, err := retry.Prepare(
			ctx,
			dumps,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		require.False(t, retryPreparation.Skipped)
		require.Zero(t, retryPreparation.ResumedFromRunID)
		require.NotEqual(t, failedPreparation.RunID, retryPreparation.RunID)
		require.NoError(t, retry.Complete(ctx, errors.New("fixture cleanup")))
	})

	t.Run("keeps force and downgrade authorization independent from revision", func(t *testing.T) {
		reset(t)
		julyDump := []*opendiscogsmodel.DiscogsDump{
			importDump(releaseEntityType, "2026-07-01", "0"),
		}
		july := NewImportExecutionCoordinator(sqlDB, "test")
		julyPreparation, err := july.Prepare(
			ctx,
			julyDump,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, julyPreparation, julyDump)
		require.NoError(t, july.Complete(ctx, nil))
		setImportContractRevision(
			t,
			db,
			julyPreparation.RunID,
			legacyImportContractRevision,
		)

		augustDump := []*opendiscogsmodel.DiscogsDump{
			importDump(releaseEntityType, "2026-08-01", "1"),
		}
		august := NewImportExecutionCoordinator(sqlDB, "test")
		augustPreparation, err := august.Prepare(
			ctx,
			augustDump,
			testChunkSize,
			false,
			false,
		)
		require.NoError(t, err)
		complete(t, augustPreparation, augustDump)
		require.NoError(t, august.Complete(ctx, nil))

		for _, force := range []bool{false, true} {
			blocked := NewImportExecutionCoordinator(sqlDB, "test")
			_, err = blocked.Prepare(
				ctx,
				julyDump,
				testChunkSize,
				force,
				false,
			)
			require.ErrorContains(t, err, "predates checkpoint")
		}

		authorized := NewImportExecutionCoordinator(sqlDB, "test")
		authorizedPreparation, err := authorized.Prepare(
			ctx,
			julyDump,
			testChunkSize,
			false,
			true,
		)
		require.NoError(t, err)
		require.False(t, authorizedPreparation.Skipped)
		require.Zero(t, authorizedPreparation.ResumedFromRunID)
		requireImportContractRevision(
			t,
			db,
			authorizedPreparation.RunID,
			currentImportContractRevisions[releaseEntityType],
		)
		require.NoError(t, authorized.Complete(ctx, errors.New("fixture cleanup")))
	})

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

	t.Run("resumes current contract progress from Java", func(t *testing.T) {
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
		require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
			if insertErr := tx.Exec(
				`insert into public.discogs_import_run_chunk
				    (import_run_id, entity_type, chunk_index, first_item_index, item_count)
				 values (?, 'release', 0, 0, 1)`,
				failedPreparation.RunID,
			).Error; insertErr != nil {
				return insertErr
			}
			return tx.Exec(
				`update public.discogs_import_run_dump
				    set processed_items = 1,
				        last_progress_at = now()
				  where import_run_id = ?
				    and entity_type = 'release'`,
				failedPreparation.RunID,
			).Error
		}))
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))
		require.NoError(t, db.Exec(
			`update public.discogs_import_run
			    set processor = ?, processor_version = ?
			  where id = ?`,
			"open-discogs-batch",
			"java-version",
			failedPreparation.RunID,
		).Error)

		retry := NewImportExecutionCoordinator(sqlDB, "go-version")
		prepared, err := retry.Prepare(ctx, dumps, testChunkSize, false, false)
		require.NoError(t, err)
		require.Equal(t, failedPreparation.RunID, prepared.ResumedFromRunID)
		require.NotEqual(t, failedPreparation.RunID, prepared.RunID)
		require.Empty(t, completedChunkIndexes(t, db, failedPreparation.RunID, "release"))
		require.ElementsMatch(
			t,
			[]int64{0},
			completedChunkIndexes(t, db, prepared.RunID, "release"),
		)
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

	t.Run("prunes progress superseded by a compatible processor", func(t *testing.T) {
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
			require.Empty(t, completedChunkIndexes(
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

	t.Run("tracks bootstrap refresh failure and aggregate readiness", func(t *testing.T) {
		reset(t)
		assertCatalogReadiness(t, db, false, "bootstrap_pending", 0)

		artistDump := []*opendiscogsmodel.DiscogsDump{
			importDump(artistEntityType, "2026-07-01", "a"),
		}
		bootstrap := NewImportExecutionCoordinator(sqlDB, "test")
		bootstrapPreparation, err := bootstrap.Prepare(
			ctx, artistDump, testChunkSize, false, false,
		)
		require.NoError(t, err)
		assertBootstrapForeignKeys(t, db, artistEntityType, 0, 0)
		assertCatalogEntityState(
			t, db, artistEntityType, catalogStatusImporting, "bootstrap",
			bootstrapPreparation.RunID, 0,
		)
		complete(t, bootstrapPreparation, artistDump)
		require.NoError(t, bootstrap.Complete(ctx, nil))
		assertBootstrapForeignKeys(t, db, artistEntityType, 8, 8)
		assertCatalogEntityState(
			t, db, artistEntityType, catalogStatusReady, "",
			0, bootstrapPreparation.RunID,
		)
		assertCatalogReadiness(t, db, false, "bootstrap_pending", 1)

		refreshDump := []*opendiscogsmodel.DiscogsDump{
			importDump(artistEntityType, "2026-08-01", "b"),
		}
		failedRefresh := NewImportExecutionCoordinator(sqlDB, "test")
		failedPreparation, err := failedRefresh.Prepare(
			ctx, refreshDump, testChunkSize, false, false,
		)
		require.NoError(t, err)
		assertBootstrapForeignKeys(t, db, artistEntityType, 8, 8)
		assertCatalogEntityState(
			t, db, artistEntityType, catalogStatusImporting, "refresh",
			failedPreparation.RunID, bootstrapPreparation.RunID,
		)
		expected := errors.New("fixture refresh failure")
		require.NoError(t, failedRefresh.Complete(ctx, expected))
		assertCatalogEntityState(
			t, db, artistEntityType, catalogStatusFailed, "refresh",
			0, bootstrapPreparation.RunID,
		)

		retry := NewImportExecutionCoordinator(sqlDB, "test")
		retryPreparation, err := retry.Prepare(
			ctx, refreshDump, testChunkSize, false, false,
		)
		require.NoError(t, err)
		complete(t, retryPreparation, refreshDump)
		require.NoError(t, retry.Complete(ctx, nil))

		remainingDumps := []*opendiscogsmodel.DiscogsDump{
			importDump(labelEntityType, "2026-08-01", "c"),
			importDump(masterEntityType, "2026-08-01", "d"),
			importDump(releaseEntityType, "2026-08-01", "e"),
		}
		remaining := NewImportExecutionCoordinator(sqlDB, "test")
		remainingPreparation, err := remaining.Prepare(
			ctx, remainingDumps, testChunkSize, false, false,
		)
		require.NoError(t, err)
		complete(t, remainingPreparation, remainingDumps)
		require.NoError(t, remaining.Complete(ctx, nil))
		assertCatalogReadiness(t, db, true, catalogStatusReady, 4)
	})
}

func assertCatalogEntityState(
	t *testing.T,
	db *gorm.DB,
	entityType string,
	status string,
	operation string,
	activeRunID int64,
	lastSuccessfulRunID int64,
) {
	t.Helper()
	var state struct {
		Status                    string
		Operation                 *string
		ActiveImportRunID         *int64
		LastSuccessfulImportRunID *int64
	}
	require.NoError(t, db.Raw(`
		select status, operation, active_import_run_id, last_successful_import_run_id
		from discogs_catalog_entity_state
		where entity_type = ?`, entityType).Scan(&state).Error)
	require.Equal(t, status, state.Status)
	if operation == "" {
		require.Nil(t, state.Operation)
	} else {
		require.Equal(t, operation, *state.Operation)
	}
	if activeRunID == 0 {
		require.Nil(t, state.ActiveImportRunID)
	} else {
		require.Equal(t, activeRunID, *state.ActiveImportRunID)
	}
	if lastSuccessfulRunID == 0 {
		require.Nil(t, state.LastSuccessfulImportRunID)
	} else {
		require.Equal(t, lastSuccessfulRunID, *state.LastSuccessfulImportRunID)
	}
}

func assertCatalogReadiness(
	t *testing.T,
	db *gorm.DB,
	ready bool,
	status string,
	readyEntities int64,
) {
	t.Helper()
	var state struct {
		Ready         bool
		Status        string
		ReadyEntities int64
	}
	require.NoError(t, db.Raw(`
		select ready, status, ready_entities
		from discogs_catalog_readiness`).Scan(&state).Error)
	require.Equal(t, ready, state.Ready)
	require.Equal(t, status, state.Status)
	require.Equal(t, readyEntities, state.ReadyEntities)
}

func assertBootstrapForeignKeys(
	t *testing.T,
	db *gorm.DB,
	entityType string,
	expectedExisting int64,
	expectedValidated int64,
) {
	t.Helper()
	var state struct {
		Existing  int64
		Validated int64
	}
	require.NoError(t, db.Raw(`
		select count(constraint_state.oid) as existing,
		       count(constraint_state.oid) filter (where constraint_state.convalidated) as validated
		from discogs_bootstrap_foreign_keys() foreign_key
		left join pg_constraint constraint_state
		  on constraint_state.conrelid = to_regclass(format('public.%I', foreign_key.table_name))
		 and constraint_state.conname = foreign_key.constraint_name
		where foreign_key.entity_type = ?`, entityType).Scan(&state).Error)
	require.Equal(t, expectedExisting, state.Existing)
	require.Equal(t, expectedValidated, state.Validated)
}

func seedPendingCatalogStates(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`
		insert into discogs_catalog_entity_state (entity_type, status, operation)
		values
		    ('artist', 'bootstrap_pending', 'bootstrap'),
		    ('label', 'bootstrap_pending', 'bootstrap'),
		    ('master', 'bootstrap_pending', 'bootstrap'),
		    ('release', 'bootstrap_pending', 'bootstrap')`).Error)
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

const (
	legacyImportContractRevision       = importContractRevision(1)
	incompatibleImportContractRevision = importContractRevision(99)
)

func installImportContractRevisionFixture(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`
		alter table public.discogs_import_run_dump
		add column if not exists import_contract_revision integer not null default 1
		check (import_contract_revision > 0);
		alter table public.discogs_import_run_dump
		alter column import_contract_revision drop default`).Error)
}

func setImportContractRevision(
	t *testing.T,
	db *gorm.DB,
	runID int64,
	revision importContractRevision,
) {
	t.Helper()
	result := db.Exec(
		`update public.discogs_import_run_dump
		    set import_contract_revision = ?
		  where import_run_id = ?`,
		revision,
		runID,
	)
	require.NoError(t, result.Error)
	require.Positive(t, result.RowsAffected)
}

func requireImportContractRevision(
	t *testing.T,
	db *gorm.DB,
	runID int64,
	want importContractRevision,
) {
	t.Helper()
	var actual importContractRevision
	require.NoError(t, db.Raw(
		`select import_contract_revision
		   from public.discogs_import_run_dump
		  where import_run_id = ?`,
		runID,
	).Scan(&actual).Error)
	require.Equal(t, want, actual)
}

func importCheckpointRunIDs(t *testing.T, db *gorm.DB) map[string]int64 {
	t.Helper()
	type checkpoint struct {
		EntityType  string
		ImportRunID int64
	}
	var checkpoints []checkpoint
	require.NoError(t, db.Raw(`
		select entity_type, import_run_id
		  from public.discogs_import_checkpoint
		 order by entity_type`).Scan(&checkpoints).Error)
	runIDs := make(map[string]int64, len(checkpoints))
	for _, checkpoint := range checkpoints {
		runIDs[checkpoint.EntityType] = checkpoint.ImportRunID
	}
	return runIDs
}

func requireRunDumpContractRevisions(
	t *testing.T,
	db *gorm.DB,
	runID int64,
	want map[string]importContractRevision,
) {
	t.Helper()
	type runDumpRevision struct {
		EntityType             string
		ImportContractRevision importContractRevision
	}
	var revisions []runDumpRevision
	require.NoError(t, db.Raw(`
		select entity_type, import_contract_revision
		  from public.discogs_import_run_dump
		 where import_run_id = ?
		 order by entity_type`, runID).Scan(&revisions).Error)
	require.Len(t, revisions, len(want))
	for _, revision := range revisions {
		require.Equal(t, want[revision.EntityType], revision.ImportContractRevision)
	}
}
