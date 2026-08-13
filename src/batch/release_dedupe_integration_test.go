package batch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestReleaseRelationPostgreSQLContracts(t *testing.T) {
	postgres := testutils.GetDatabase(t, testutils.Postgres)
	dsn := testutils.GetDsn(testutils.Postgres, postgres)
	tests := []struct {
		name string
		run  func(*testing.T, string)
	}{
		{"overlapping master lock order", runConcurrentReleaseChunksLockOverlappingMastersInOneOrder},
		{"hash collision idempotency", runReleaseRelationWriterPersistsHashCollisionsAndRetriesIdempotently},
		{"legacy identity reconciliation", runReleaseRelationReconciliationBackfillsLegacyIdentityAndRetainsRows},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache.ResetIDs()
			t.Cleanup(cache.ResetIDs)
			test.run(t, dsn)
		})
	}
}

func resetReleaseRelationDatabase(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	db, err := database.GetConnect(dsn)
	require.NoError(t, err)
	require.NoError(t, db.Exec("drop schema public cascade").Error)
	require.NoError(t, db.Exec("create schema public").Error)
	require.NoError(t, RunDDL(db))
	initialSQLDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, initialSQLDB.Close())
	db, err = database.GetConnect(dsn)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	return db
}

func runConcurrentReleaseChunksLockOverlappingMastersInOneOrder(t *testing.T, dsn string) {
	db := resetReleaseRelationDatabase(t, dsn)

	const (
		firstMasterID int32 = 2_000_000_100
		masterCount         = 4
		workerCount         = 4
	)
	now := time.Now().UTC()
	for offset := int32(0); offset < masterCount; offset++ {
		masterID := firstMasterID + offset
		cache.MasterIDs.Add(masterID)
		require.NoError(t, db.Create(&model.Master{
			ID: masterID, CreatedAt: now, LastModifiedAt: now,
		}).Error)
	}

	start := make(chan struct{})
	errorsByWorker := make(chan error, workerCount)
	var workers sync.WaitGroup
	for worker := int32(0); worker < workerCount; worker++ {
		workers.Add(1)
		go func(workerID int32) {
			defer workers.Done()
			items := make([]*XmlReleaseRelation, 0, masterCount)
			for index := int32(0); index < masterCount; index++ {
				masterOffset := (index + workerID) % masterCount
				masterID := firstMasterID + masterOffset
				releaseID := int32(2_000_001_000) + workerID*masterCount + masterOffset
				title := fmt.Sprintf("worker-%d-master-%d", workerID, masterOffset)
				items = append(items, &XmlReleaseRelation{
					ID: releaseID, Title: &title,
					MasterInfo: XmlReleaseMasterInfo{MasterID: &masterID, IsMaster: true},
				})
			}
			<-start
			written := writeReleaseRelationChunk(
				NewOrder(context.Background(), masterCount, 1, "unused", db),
				ChunkMetadata{Index: int64(workerID), ItemCount: masterCount},
				items,
			)
			errorsByWorker <- written.Err()
		}(worker)
	}
	close(start)
	workers.Wait()
	close(errorsByWorker)
	for writeError := range errorsByWorker {
		require.NoError(t, writeError)
	}

	var linkedMasters int64
	require.NoError(t, db.Model(&model.Master{}).
		Where("id >= ? and id < ? and main_release_id is not null", firstMasterID, firstMasterID+masterCount).
		Count(&linkedMasters).Error)
	require.Equal(t, int64(masterCount), linkedMasters)
}

func runReleaseRelationWriterPersistsHashCollisionsAndRetriesIdempotently(
	t *testing.T,
	dsn string,
) {
	db := resetReleaseRelationDatabase(t, dsn)

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

	collisionResult := writeReleaseRelationBatch(
		conflicting,
		len(conflicting),
		db,
		deduplicateReleaseFormats,
	)
	require.NoError(t, collisionResult.Err())
	require.Equal(t, 2, collisionResult.Count())
	requireRelationRowCount(t, db, &model.ReleaseItemFormat{}, releaseItemID, 2)

	collisionRetry := writeReleaseRelationBatch(
		conflicting,
		len(conflicting),
		db,
		deduplicateReleaseFormats,
	)
	require.NoError(t, collisionRetry.Err())
	require.Zero(t, collisionRetry.Count())
	requireRelationRowCount(t, db, &model.ReleaseItemFormat{}, releaseItemID, 2)

	exactDuplicates := []*model.ReleaseItemFormat{
		{
			ReleaseItemID:  releaseItemID,
			Hash:           202,
			Name:           &name,
			Quantity:       &quantityOne,
			CreatedAt:      now,
			LastModifiedAt: now,
		},
		{
			ID:             99,
			ReleaseItemID:  releaseItemID,
			Hash:           202,
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
	requireRelationRowCount(t, db, &model.ReleaseItemFormat{}, releaseItemID, 3)

	retry := writeReleaseRelationBatch(exactDuplicates, 1, db, deduplicateReleaseFormats)
	require.NoError(t, retry.Err())
	require.Zero(t, retry.Count())
	requireRelationRowCount(t, db, &model.ReleaseItemFormat{}, releaseItemID, 3)

	cache.ArtistIDs.Add(1)
	cache.LabelIDs.Add(5)
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

	fixtureWrite := writeReleaseRelationChunk(order, ChunkMetadata{}, []*XmlReleaseRelation{fixture})
	require.NoError(t, fixtureWrite.Err())
	require.NotZero(t, fixtureWrite.Count())
	requireReleaseFixtureRelationCounts(t, db, fixture.ID)

	fixtureRetry := writeReleaseRelationChunk(order, ChunkMetadata{}, []*XmlReleaseRelation{fixture})
	require.NoError(t, fixtureRetry.Err())
	require.Zero(t, fixtureRetry.Count())
	requireReleaseFixtureRelationCounts(t, db, fixture.ID)
}

func runReleaseRelationReconciliationBackfillsLegacyIdentityAndRetainsRows(
	t *testing.T,
	dsn string,
) {
	db := resetReleaseRelationDatabase(t, dsn)

	const (
		releaseItemID = int32(2_000_000_002)
		legacyHash    = int32(86_171)
	)
	now := time.Now().UTC()
	require.NoError(t, db.Create(&model.ReleaseItem{
		ID:             releaseItemID,
		CreatedAt:      now,
		LastModifiedAt: now,
	}).Error)
	firstPosition := "6"
	firstTitle := "Яд"
	require.NoError(t, db.Create(&model.ReleaseItemTrack{
		ID:             2_000_000_100,
		ReleaseItemID:  releaseItemID,
		Hash:           legacyHash,
		Position:       &firstPosition,
		Title:          &firstTitle,
		CreatedAt:      now,
		LastModifiedAt: now,
	}).Error)

	secondPosition := "7"
	secondTitle := "Ад"
	release := &XmlReleaseRelation{
		ID: releaseItemID,
		Tracks: []XmlTrack{
			{Position: firstPosition, Title: firstTitle},
			{Position: secondPosition, Title: secondTitle},
		},
	}
	firstWrite := writeReleaseRelationChunk(
		NewOrder(context.Background(), 10, 1, "unused", db),
		ChunkMetadata{ItemCount: 1},
		[]*XmlReleaseRelation{release},
	)
	require.NoError(t, firstWrite.Err())

	var firstIDs []int32
	require.NoError(t, db.Model(&model.ReleaseItemTrack{}).
		Where("release_item_id = ?", releaseItemID).
		Order("id").
		Pluck("id", &firstIDs).Error)
	require.Len(t, firstIDs, 2)
	var identities [][]byte
	require.NoError(t, db.Model(&model.ReleaseItemTrack{}).
		Where("release_item_id = ?", releaseItemID).
		Order("id").
		Pluck("identity_sha256", &identities).Error)
	require.Len(t, identities, 2)
	require.Len(t, identities[0], 32)
	require.Len(t, identities[1], 32)
	require.NotEqual(t, identities[0], identities[1])

	retry := writeReleaseRelationChunk(
		NewOrder(context.Background(), 10, 1, "unused", db),
		ChunkMetadata{ItemCount: 1},
		[]*XmlReleaseRelation{release},
	)
	require.NoError(t, retry.Err())
	var retryIDs []int32
	require.NoError(t, db.Model(&model.ReleaseItemTrack{}).
		Where("release_item_id = ?", releaseItemID).
		Order("id").
		Pluck("id", &retryIDs).Error)
	require.Equal(t, firstIDs, retryIDs)

	var collidedID int32
	require.NoError(t, db.Model(&model.ReleaseItemTrack{}).
		Where("release_item_id = ? and title = ?", releaseItemID, secondTitle).
		Select("id").Scan(&collidedID).Error)
	reducedRelease := &XmlReleaseRelation{
		ID: releaseItemID,
		Tracks: []XmlTrack{
			{Position: secondPosition, Title: secondTitle},
		},
	}
	reassigned := writeReleaseRelationChunk(
		NewOrder(context.Background(), 10, 1, "unused", db),
		ChunkMetadata{ItemCount: 1},
		[]*XmlReleaseRelation{reducedRelease},
	)
	require.NoError(t, reassigned.Err())
	var remaining model.ReleaseItemTrack
	require.NoError(t, db.Where("release_item_id = ?", releaseItemID).First(&remaining).Error)
	require.NotEqual(t, collidedID, remaining.ID)
	require.Equal(t, legacyHash, remaining.Hash)

	stable := writeReleaseRelationChunk(
		NewOrder(context.Background(), 10, 1, "unused", db),
		ChunkMetadata{ItemCount: 1},
		[]*XmlReleaseRelation{reducedRelease},
	)
	require.NoError(t, stable.Err())
	var stableID int32
	require.NoError(t, db.Model(&model.ReleaseItemTrack{}).
		Where("release_item_id = ?", releaseItemID).
		Select("id").Scan(&stableID).Error)
	require.Equal(t, remaining.ID, stableID)
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
