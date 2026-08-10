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
)

func TestImportExecutionCoordinator(t *testing.T) {
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

	t.Run("skips a successful manifest unless forced", func(t *testing.T) {
		reset(t)
		dumps := []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "a"),
		}

		first := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := first.Prepare(ctx, dumps, false, false)
		require.NoError(t, err)
		require.False(t, prepared.Skipped)
		require.NoError(t, first.Complete(ctx, nil))

		repeated := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err = repeated.Prepare(ctx, dumps, false, false)
		require.NoError(t, err)
		require.True(t, prepared.Skipped)

		forced := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err = forced.Prepare(ctx, dumps, true, false)
		require.NoError(t, err)
		require.False(t, prepared.Skipped)
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
		_, err := artistRelease.Prepare(ctx, []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "b"),
			importDump("release", "2026-07-01", "c"),
		}, false, false)
		require.NoError(t, err)

		master := NewImportExecutionCoordinator(sqlDB, "test")
		_, err = master.Prepare(ctx, []*opendiscogsmodel.DiscogsDump{
			importDump("master", "2026-07-01", "d"),
		}, false, false)
		require.NoError(t, err)

		overlapping := NewImportExecutionCoordinator(sqlDB, "test")
		_, err = overlapping.Prepare(ctx, []*opendiscogsmodel.DiscogsDump{
			importDump("artist", "2026-07-01", "e"),
		}, false, false)
		require.ErrorContains(t, err, "already updating artist")

		require.NoError(t, master.Complete(ctx, nil))
		require.NoError(t, artistRelease.Complete(ctx, nil))
	})

	t.Run("rejects downgrades unless separately authorized", func(t *testing.T) {
		reset(t)
		newer := NewImportExecutionCoordinator(sqlDB, "test")
		_, err := newer.Prepare(ctx, []*opendiscogsmodel.DiscogsDump{
			importDump("label", "2026-07-01", "f"),
		}, false, false)
		require.NoError(t, err)
		require.NoError(t, newer.Complete(ctx, nil))

		olderDump := []*opendiscogsmodel.DiscogsDump{
			importDump("label", "2026-06-01", "1"),
		}
		older := NewImportExecutionCoordinator(sqlDB, "test")
		_, err = older.Prepare(ctx, olderDump, true, false)
		require.ErrorContains(t, err, "predates checkpoint")

		authorized := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := authorized.Prepare(ctx, olderDump, false, true)
		require.NoError(t, err)
		require.False(t, prepared.Skipped)
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
		_, err := failed.Prepare(ctx, dumps, false, false)
		require.NoError(t, err)
		require.NoError(t, failed.Complete(ctx, errors.New("fixture failure")))

		retry := NewImportExecutionCoordinator(sqlDB, "test")
		prepared, err := retry.Prepare(ctx, dumps, false, false)
		require.NoError(t, err)
		require.False(t, prepared.Skipped)
		require.NoError(t, retry.Complete(ctx, nil))
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
