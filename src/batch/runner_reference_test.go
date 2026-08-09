package batch

import (
	"context"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
)

func TestPreloadReferenceIDsForReleaseOnlyImport(t *testing.T) {
	pg := testutils.GetDatabase(testutils.Postgres)
	db, err := database.GetConnect(testutils.GetDsn(testutils.Postgres, pg))
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))
	const (
		artistID int32 = 2_000_000_001
		labelID  int32 = 2_000_000_002
		masterID int32 = 2_000_000_003
	)
	require.NoError(t, db.Exec(
		"INSERT INTO public.artist (id, created_at, last_modified_at) VALUES (?, now(), now()) ON CONFLICT DO NOTHING",
		artistID,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO public.label (id, created_at, last_modified_at) VALUES (?, now(), now()) ON CONFLICT DO NOTHING",
		labelID,
	).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO public.master (id, created_at, last_modified_at) VALUES (?, now(), now()) ON CONFLICT DO NOTHING",
		masterID,
	).Error)
	t.Cleanup(func() {
		db.Exec("DELETE FROM public.master WHERE id = ?", masterID)
		db.Exec("DELETE FROM public.label WHERE id = ?", labelID)
		db.Exec("DELETE FROM public.artist WHERE id = ?", artistID)
		cache.ResetIDs()
	})

	config := koanf.New(".")
	require.NoError(t, config.Set("artists", false))
	require.NoError(t, config.Set("labels", false))
	require.NoError(t, config.Set("masters", false))
	require.NoError(t, config.Set("releases", true))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	cache.ResetIDs()

	require.NoError(t, preloadReferenceIDs(context.Background(), sqlDB, config))
	require.True(t, cache.ArtistIDs.Contains(artistID))
	require.True(t, cache.LabelIDs.Contains(labelID))
	require.True(t, cache.MasterIDs.Contains(masterID))
}
