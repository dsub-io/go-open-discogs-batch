package batch

import (
	"context"
	"database/sql"
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
		{"master backlink reconciliation", runMasterMainReleaseReconciliationConvergesAndRollsBack},
		{"hash collision idempotency", runReleaseRelationWriterPersistsHashCollisionsAndRetriesIdempotently},
		{"non-release hash collision idempotency", runArtistRelationWriterPersistsHashCollisionsAndRetriesIdempotently},
		{"legacy identity reconciliation", runReleaseRelationReconciliationBackfillsLegacyIdentityAndRetainsRows},
		{"unchanged root", runUnchangedRootSkipsPostgreSQLUpdate},
		{"changed dump convergence", runChangedDumpConvergesRootAndRelations},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache.ResetIDs()
			t.Cleanup(cache.ResetIDs)
			test.run(t, dsn)
		})
	}
}

func runArtistRelationWriterPersistsHashCollisionsAndRetriesIdempotently(
	t *testing.T,
	dsn string,
) {
	db := resetReleaseRelationDatabase(t, dsn)
	const artistID = int32(33476)
	now := time.Now().UTC()
	require.NoError(t, db.Exec(
		`INSERT INTO artist (id, created_at, last_modified_at) VALUES (?, ?, ?)`,
		artistID,
		now,
		now,
	).Error)
	order := NewOrder(context.Background(), 10, 1, "unused", db)
	items := []*XmlArtistRelation{{
		ID:       artistID,
		NameVars: []string{"Al Thompson", "C. Thompson"},
	}}

	first := writeArtistRelationChunk(order, ChunkMetadata{ItemCount: 1}, items)
	require.NoError(t, first.Err())
	require.Equal(t, 2, first.Count())

	var rows []*model.ArtistNameVariation
	require.NoError(t, db.Where("artist_id = ?", artistID).Order("name_variation").Find(&rows).Error)
	require.Len(t, rows, 2)
	require.Equal(t, "Al Thompson", rows[0].NameVariation)
	require.Equal(t, "C. Thompson", rows[1].NameVariation)
	require.NotEqual(t, rows[0].Hash, rows[1].Hash)
	require.NotNil(t, rows[0].IdentitySHA256)
	require.NotNil(t, rows[1].IdentitySHA256)
	require.NotEqual(t, rows[0].IdentitySHA256, rows[1].IdentitySHA256)

	retry := writeArtistRelationChunk(order, ChunkMetadata{ItemCount: 1}, items)
	require.NoError(t, retry.Err())
	require.Zero(t, retry.Count())
}

func runChangedDumpConvergesRootAndRelations(t *testing.T, dsn string) {
	db := resetReleaseRelationDatabase(t, dsn)
	observedAt := time.Now().UTC()
	initialName := "Artist"
	initial := &model.Artist{
		ID:             1,
		CreatedAt:      observedAt,
		LastModifiedAt: observedAt,
		Name:           &initialName,
	}
	recorded := writeCanonicalBatch([]*model.Artist{initial}, 1, db, deduplicateArtists)
	require.NoError(t, recorded.Err())

	order := NewOrder(context.Background(), 10, 1, "unused", db)
	firstRelations := writeArtistRelationChunk(
		order,
		ChunkMetadata{ItemCount: 1},
		[]*XmlArtistRelation{{
			ID:   initial.ID,
			URLs: []string{"https://a.example", "https://b.example"},
		}},
	)
	require.NoError(t, firstRelations.Err())

	changedName := "Changed Artist"
	changedAt := observedAt.Add(time.Second)
	changedRoot := &model.Artist{
		ID:             initial.ID,
		CreatedAt:      changedAt,
		LastModifiedAt: changedAt,
		Name:           &changedName,
	}
	rootUpdate := writeCanonicalBatch([]*model.Artist{changedRoot}, 1, db, deduplicateArtists)
	require.NoError(t, rootUpdate.Err())
	require.Equal(t, 1, rootUpdate.Count())

	changedRelations := writeArtistRelationChunk(
		order,
		ChunkMetadata{ItemCount: 1},
		[]*XmlArtistRelation{{
			ID:   initial.ID,
			URLs: []string{"https://b.example", "https://c.example"},
		}},
	)
	require.NoError(t, changedRelations.Err())

	var storedRoot model.Artist
	require.NoError(t, db.First(&storedRoot, initial.ID).Error)
	require.Equal(t, changedName, *storedRoot.Name)
	var storedURLs []*model.ArtistURL
	require.NoError(t, db.Where("artist_id = ?", initial.ID).Order("url").Find(&storedURLs).Error)
	require.Len(t, storedURLs, 2)
	require.Equal(t, "https://b.example", storedURLs[0].URL)
	require.Equal(t, "https://c.example", storedURLs[1].URL)
}

func runUnchangedRootSkipsPostgreSQLUpdate(t *testing.T, dsn string) {
	db := resetReleaseRelationDatabase(t, dsn)
	observedAt := time.Now().UTC()
	name := "Artist"
	initial := &model.Artist{
		ID:             1,
		CreatedAt:      observedAt,
		LastModifiedAt: observedAt,
		Name:           &name,
	}
	recorded := writeCanonicalBatch([]*model.Artist{initial}, 1, db, deduplicateArtists)
	require.NoError(t, recorded.Err())
	require.Equal(t, 1, recorded.Count())

	require.NoError(t, db.Exec(`
		CREATE FUNCTION reject_artist_update() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN
			RAISE EXCEPTION 'unchanged root must not update';
		END $$`).Error)
	require.NoError(t, db.Exec(`
		CREATE TRIGGER reject_artist_update
		BEFORE UPDATE ON artist
		FOR EACH ROW EXECUTE FUNCTION reject_artist_update()`).Error)

	unchanged := &model.Artist{
		ID:             initial.ID,
		CreatedAt:      observedAt.Add(time.Second),
		LastModifiedAt: observedAt.Add(time.Second),
		Name:           &name,
	}
	retry := writeCanonicalBatch([]*model.Artist{unchanged}, 1, db, deduplicateArtists)
	require.NoError(t, retry.Err())
	require.Zero(t, retry.Count())

	changedName := "Changed Artist"
	unchanged.Name = &changedName
	changed := writeCanonicalBatch([]*model.Artist{unchanged}, 1, db, deduplicateArtists)
	require.ErrorContains(t, changed.Err(), "unchanged root must not update")
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

func runMasterMainReleaseReconciliationConvergesAndRollsBack(t *testing.T, dsn string) {
	db := resetReleaseRelationDatabase(t, dsn)

	const (
		masterA  int32 = 2_000_000_100
		masterB  int32 = 2_000_000_101
		masterC  int32 = 2_000_000_102
		releaseA int32 = 2_000_001_000
		releaseB int32 = 2_000_001_001
		releaseC int32 = 2_000_001_002
	)
	now := time.Now().UTC()
	require.NoError(t, db.Exec(
		`INSERT INTO master (id, created_at, last_modified_at)
		 VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)`,
		masterA, now, now, masterB, now, now, masterC, now, now,
	).Error)
	require.NoError(t, db.Exec(
		`INSERT INTO release_item
		     (id, created_at, last_modified_at, master_id, is_master)
		 VALUES (?, ?, ?, ?, true), (?, ?, ?, ?, true), (?, ?, ?, null, false)`,
		releaseA, now, now, masterA,
		releaseB, now, now, masterC,
		releaseC, now, now,
	).Error)
	order := NewOrder(context.Background(), 1, 1, "unused", db)

	first := reconcileMasterMainReleases(order)
	require.NoError(t, first.Err())
	require.Equal(t, releaseA, masterMainReleaseID(t, db, masterA))
	require.Equal(t, releaseB, masterMainReleaseID(t, db, masterC))

	require.NoError(t, db.Exec(
		`UPDATE release_item
		    SET master_id = ?, last_modified_at = ?
		  WHERE id = ?`,
		masterB, now.Add(time.Second), releaseA,
	).Error)
	require.NoError(t, reconcileMasterMainReleases(order).Err())
	require.Zero(t, masterMainReleaseID(t, db, masterA))
	require.Equal(t, releaseA, masterMainReleaseID(t, db, masterB))

	require.NoError(t, db.Exec(
		`UPDATE release_item
		    SET is_master = false, last_modified_at = ?
		  WHERE id = ?`,
		now.Add(2*time.Second), releaseA,
	).Error)
	require.NoError(t, reconcileMasterMainReleases(order).Err())
	require.Zero(t, masterMainReleaseID(t, db, masterB))

	require.NoError(t, db.Exec(
		`UPDATE release_item
		    SET master_id = ?, is_master = true, last_modified_at = ?
		  WHERE id = ?`,
		masterC, now.Add(3*time.Second), releaseC,
	).Error)
	require.NoError(t, db.Exec(
		`CREATE FUNCTION reject_release_c_backlink() RETURNS trigger
		 LANGUAGE plpgsql AS $$
		 BEGIN
		   IF NEW.main_release_id = 2000001002 THEN
		     RAISE EXCEPTION 'rejected release C backlink';
		   END IF;
		   RETURN NEW;
		 END
		 $$;
		 CREATE TRIGGER reject_release_c_backlink
		 BEFORE UPDATE OF main_release_id ON master
		 FOR EACH ROW EXECUTE FUNCTION reject_release_c_backlink()`,
	).Error)
	require.ErrorContains(t, reconcileMasterMainReleases(order).Err(), "rejected release C backlink")
	require.Equal(t, releaseB, masterMainReleaseID(t, db, masterC))
	require.NoError(t, db.Exec(
		`DROP TRIGGER reject_release_c_backlink ON master;
		 DROP FUNCTION reject_release_c_backlink()`,
	).Error)
	require.NoError(t, reconcileMasterMainReleases(order).Err())
	require.Equal(t, releaseC, masterMainReleaseID(t, db, masterC))
	require.Zero(t, reconcileMasterMainReleases(order).Count())
}

func masterMainReleaseID(t *testing.T, db *gorm.DB, masterID int32) int32 {
	t.Helper()
	var releaseID sql.NullInt64
	require.NoError(t, db.Raw(
		"SELECT main_release_id FROM master WHERE id = ?", masterID,
	).Scan(&releaseID).Error)
	if !releaseID.Valid {
		return 0
	}
	return int32(releaseID.Int64)
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
			LastModifiedAt: now,
		},
		{
			ReleaseItemID:  releaseItemID,
			Hash:           101,
			Name:           &name,
			Quantity:       &quantityTwo,
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
			LastModifiedAt: now,
		},
		{
			ReleaseItemID:  releaseItemID,
			Hash:           202,
			Name:           &name,
			Quantity:       &quantityOne,
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
		ReleaseItemID:  releaseItemID,
		Hash:           legacyHash,
		Position:       &firstPosition,
		Title:          &firstTitle,
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

	var firstTracks []model.ReleaseItemTrack
	require.NoError(t, db.Where("release_item_id = ?", releaseItemID).
		Order("title").Find(&firstTracks).Error)
	require.Len(t, firstTracks, 2)
	require.Len(t, firstTracks[0].IdentitySHA256, 32)
	require.Len(t, firstTracks[1].IdentitySHA256, 32)
	require.NotEqual(t, firstTracks[0].IdentitySHA256, firstTracks[1].IdentitySHA256)

	retry := writeReleaseRelationChunk(
		NewOrder(context.Background(), 10, 1, "unused", db),
		ChunkMetadata{ItemCount: 1},
		[]*XmlReleaseRelation{release},
	)
	require.NoError(t, retry.Err())
	var retryTracks []model.ReleaseItemTrack
	require.NoError(t, db.Where("release_item_id = ?", releaseItemID).
		Order("title").Find(&retryTracks).Error)
	require.Equal(t, firstTracks, retryTracks)

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
	require.Equal(t, legacyHash, remaining.Hash)
	require.Equal(t, secondTitle, *remaining.Title)
	require.Len(t, remaining.IdentitySHA256, 32)

	stable := writeReleaseRelationChunk(
		NewOrder(context.Background(), 10, 1, "unused", db),
		ChunkMetadata{ItemCount: 1},
		[]*XmlReleaseRelation{reducedRelease},
	)
	require.NoError(t, stable.Err())
	var stableTrack model.ReleaseItemTrack
	require.NoError(t, db.Where("release_item_id = ?", releaseItemID).First(&stableTrack).Error)
	require.Equal(t, remaining, stableTrack)
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
