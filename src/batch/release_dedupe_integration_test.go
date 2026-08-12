package batch

import (
	"context"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestReleaseRelationWriterPreventsPostgresSQLState21000AndRetriesIdempotently(t *testing.T) {
	postgres := testutils.GetDatabase(t, testutils.Postgres)
	dsn := testutils.GetDsn(testutils.Postgres, postgres)
	db, err := database.GetConnect(dsn)
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))
	db, err = database.GetConnect(dsn)
	require.NoError(t, err)

	now := time.Now().UTC()
	const releaseItemID int32 = 2_000_000_001
	require.NoError(t, db.Create(&model.ReleaseItem{
		ID:             releaseItemID,
		CreatedAt:      now,
		LastModifiedAt: now,
	}).Error)

	name := "Vinyl"
	quantityOne := int32(1)
	quantityTwo := int32(2)
	conflicting := []*model.ReleaseItemFormat{
		{
			ReleaseItemID:  releaseItemID,
			Hash:           101,
			Name:           &name,
			Quantity:       &quantityOne,
			CreatedAt:      now,
			LastModifiedAt: now,
		},
		{
			ReleaseItemID:  releaseItemID,
			Hash:           101,
			Name:           &name,
			Quantity:       &quantityTwo,
			CreatedAt:      now.Add(time.Second),
			LastModifiedAt: now.Add(time.Second),
		},
	}

	conflictResult := writeReleaseRelationBatch(
		conflicting,
		len(conflicting),
		db,
		deduplicateReleaseFormats,
	)
	require.ErrorContains(t, conflictResult.Err(), "conflicting release_item_format rows")
	require.ErrorContains(t, conflictResult.Err(), "canonical key")
	require.NotContains(t, conflictResult.Err().Error(), "SQLSTATE 21000")
	requireRelationRowCount(t, db, &model.ReleaseItemFormat{}, releaseItemID, 0)

	exactDuplicates := []*model.ReleaseItemFormat{
		{
			ReleaseItemID:  releaseItemID,
			Hash:           101,
			Name:           &name,
			Quantity:       &quantityOne,
			CreatedAt:      now,
			LastModifiedAt: now,
		},
		{
			ID:             99,
			ReleaseItemID:  releaseItemID,
			Hash:           101,
			Name:           &name,
			Quantity:       &quantityOne,
			CreatedAt:      now.Add(time.Minute),
			LastModifiedAt: now.Add(time.Minute),
		},
	}

	firstWrite := writeReleaseRelationBatch(
		exactDuplicates,
		len(exactDuplicates),
		db,
		deduplicateReleaseFormats,
	)
	require.NoError(t, firstWrite.Err())
	require.Equal(t, 1, firstWrite.Count())
	requireRelationRowCount(t, db, &model.ReleaseItemFormat{}, releaseItemID, 1)

	retry := writeReleaseRelationBatch(exactDuplicates, 1, db, deduplicateReleaseFormats)
	require.NoError(t, retry.Err())
	require.Zero(t, retry.Count())
	requireRelationRowCount(t, db, &model.ReleaseItemFormat{}, releaseItemID, 1)

	cache.ArtistIDs.Add(1)
	cache.LabelIDs.Add(5)
	t.Cleanup(cache.ResetIDs)
	require.NoError(t, db.Create(&model.Artist{
		ID:             1,
		CreatedAt:      now,
		LastModifiedAt: now,
	}).Error)
	require.NoError(t, db.Create(&model.Label{
		ID:             5,
		CreatedAt:      now,
		LastModifiedAt: now,
	}).Error)
	fixture := readReleaseRelationDeduplicationFixture(t)
	order := NewOrder(context.Background(), 100, 1, "unused", db)

	fixtureWrite := writeReleaseRelationChunk(order, ChunkMetadata{}, []*XmlReleaseRelation{fixture}, false)
	require.NoError(t, fixtureWrite.Err())
	require.NotZero(t, fixtureWrite.Count())
	requireReleaseFixtureRelationCounts(t, db, fixture.ID)

	fixtureRetry := writeReleaseRelationChunk(order, ChunkMetadata{}, []*XmlReleaseRelation{fixture}, false)
	require.NoError(t, fixtureRetry.Err())
	require.Zero(t, fixtureRetry.Count())
	requireReleaseFixtureRelationCounts(t, db, fixture.ID)
}

func requireRelationRowCount(
	t *testing.T,
	db *gorm.DB,
	modelValue *model.ReleaseItemFormat,
	releaseItemID int32,
	expected int64,
) {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(modelValue).
		Where("release_item_id = ?", releaseItemID).
		Count(&count).Error)
	require.Equal(t, expected, count)
}

func requireReleaseFixtureRelationCounts(t *testing.T, db *gorm.DB, releaseItemID int32) {
	t.Helper()
	expected := []struct {
		table string
		count int64
	}{
		{model.ReleaseItemArtist{}.TableName(), 1},
		{model.ReleaseItemCreditedArtist{}.TableName(), 1},
		{model.ReleaseItemWork{}.TableName(), 1},
		{model.ReleaseItemFormat{}.TableName(), 1},
		{model.ReleaseItemGenre{}.TableName(), 1},
		{model.ReleaseItemStyle{}.TableName(), 1},
		{model.ReleaseItemIdentifier{}.TableName(), 1},
		{model.ReleaseItemTrack{}.TableName(), 1},
		{model.ReleaseItemVideo{}.TableName(), 1},
		{model.LabelReleaseItem{}.TableName(), 3},
	}
	for _, relation := range expected {
		var count int64
		require.NoError(t, db.Table(relation.table).
			Where("release_item_id = ?", releaseItemID).
			Count(&count).Error)
		require.Equal(t, relation.count, count, relation.table)
	}
}
