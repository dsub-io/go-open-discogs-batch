package batch

import (
	"compress/gzip"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/reactivex/rxgo/v2"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func testString(value string) *string { return &value }

type existingRelationRootFixture struct {
	table  string
	rootID int32
}

func expectExistingRelationRoots(
	mock sqlmock.Sqlmock,
	firstTable string,
	fixtures ...existingRelationRootFixture,
) {
	rows := sqlmock.NewRows([]string{"relation_table", "root_id"})
	for _, fixture := range fixtures {
		rows.AddRow(fixture.table, fixture.rootID)
	}
	mock.ExpectQuery("select '" + firstTable + "'").WithArgs(sqlmock.AnyArg()).WillReturnRows(rows)
}

func TestBatchConstructor(t *testing.T) {
	require.NotNil(t, New())
}

func TestLegacyCoreTransforms(t *testing.T) {
	observedAt := time.Unix(1, 0).UTC()
	master := &XmlMaster{ID: 1, Title: testString("master")}
	masterResult := master.TransformAt(observedAt)
	require.Equal(t, int32(1), masterResult.ID)
	require.Equal(t, observedAt, masterResult.CreatedAt)

	release := &XmlRelease{ID: 2, Title: testString("release")}
	releaseResult := release.TransformAt(observedAt)
	require.Equal(t, int32(2), releaseResult.ID)
	require.Equal(t, observedAt, releaseResult.CreatedAt)
}

func TestXMLRelationFiltersInvalidValues(t *testing.T) {
	cache.ResetIDs()
	t.Cleanup(cache.ResetIDs)
	cache.ArtistIDs.Add(1)
	cache.LabelIDs.Add(2)
	cache.MasterIDs.Add(3)

	artist := &XmlArtistRelation{
		ID:       10,
		URLs:     []string{" ", "https://example.test"},
		NameVars: []string{" ", "Name"},
		Aliases:  []XmlRef{{ID: 99}, {ID: 1}},
		Members:  []XmlRef{{ID: 99}, {ID: 1}},
	}
	require.Len(t, artist.GetUrls(), 1)
	require.Len(t, artist.GetNameVars(), 1)
	require.Len(t, artist.GetAliases(), 1)
	require.Len(t, artist.GetMembers(), 1)

	label := &XmlLabelRelation{
		ID:        11,
		URLs:      []string{" ", "https://example.test"},
		SubLabels: []XmlRef{{ID: 99}, {ID: 2}},
	}
	require.Len(t, label.GetUrls(), 1)
	require.Len(t, label.GetSubLabels(), 1)

	master := &XmlMasterRelation{
		ID:      12,
		Styles:  []string{" ", "Rock"},
		Genres:  []string{" ", "Electronic"},
		Artists: []int32{99, 1},
		Videos:  []XmlVideo{{}, {URL: "https://example.test"}},
	}
	require.Len(t, master.GetMasterStyles(), 1)
	require.Len(t, master.GetMasterGenres(), 1)
	require.Len(t, master.GetMasterArtists(), 1)
	require.Len(t, master.GetMasterVideos(), 1)

	release := &XmlReleaseRelation{
		ID: 13,
		Works: []XmlWork{
			{LabelID: 2, Work: " "},
			{LabelID: 99, Work: "Company"},
			{LabelID: 2, Work: "Company"},
		},
		Videos:          []XmlVideo{{}, {URL: "https://example.test"}},
		Identifiers:     []XmlIdentifier{{}, {Type: "Barcode", Value: "1"}},
		Tracks:          []XmlTrack{{}, {Position: "A1", Title: "Track"}},
		Formats:         []XmlFormat{{}, {Name: testString("LP")}},
		CreditedArtists: []XmlCreditedArtist{{ArtistID: 99, Role: "Role"}, {ArtistID: 1}, {ArtistID: 1, Role: "Role"}},
		Artists:         []int32{99, 1},
		Labels:          []XmlLabelRelease{{LabelID: 99}, {LabelID: 2}},
		Styles:          []string{" ", "Rock"},
		Genres:          []string{" ", "Electronic"},
	}
	require.Len(t, release.GetWorks(), 1)
	require.Len(t, release.GetVideos(), 1)
	require.Len(t, release.GetIdentifiers(), 1)
	require.Len(t, release.GetTracks(), 1)
	require.Len(t, release.GetFormats(), 1)
	require.Len(t, release.GetCreditedArtists(), 1)
	require.Len(t, release.GetReleaseArtists(), 1)
	require.Len(t, release.GetLabels(), 1)
	require.Len(t, release.GetReleaseStyles(), 1)
	require.Len(t, release.GetReleaseGenres(), 1)
}

func TestXMLValueBoundaries(t *testing.T) {
	require.Nil(t, releaseItem(
		1, nil, nil, nil, nil, nil, XmlReleaseMasterInfo{}, nil, time.Time{},
	).MasterID)
	unknownMaster := int32(99)
	require.Nil(t, releaseItem(
		1,
		nil,
		nil,
		nil,
		nil,
		nil,
		XmlReleaseMasterInfo{MasterID: &unknownMaster},
		nil,
		time.Time{},
	).MasterID)

	invalidDate := "invalid"
	parsed, validYear, validMonth, validDay := parsedReleaseDate(&invalidDate)
	require.Nil(t, parsed)
	require.False(t, validYear)
	require.False(t, validMonth)
	require.False(t, validDay)

	yearOnly := "2026"
	parsed, validYear, validMonth, validDay = parsedReleaseDate(&yearOnly)
	require.NotNil(t, parsed)
	require.True(t, validYear)
	require.False(t, validMonth)
	require.False(t, validDay)

	require.Nil(t, reducedDescription([]string{" "}))
	require.Empty(t, stringValue(nil))
}

func TestWriterDispatchesEverySupportedType(t *testing.T) {
	writer := gormWriter{}
	result := writer.Write(1,
		[]*opendiscogsmodel.Artist{},
		[]*opendiscogsmodel.ArtistURL{},
		[]*opendiscogsmodel.ArtistAlias{},
		[]*opendiscogsmodel.ArtistGroup{},
		[]*opendiscogsmodel.ArtistMember{},
		[]*opendiscogsmodel.ArtistNameVariation{},
		[]*opendiscogsmodel.Label{},
		[]*opendiscogsmodel.LabelURL{},
		[]*opendiscogsmodel.LabelSubLabel{},
		[]*opendiscogsmodel.LabelReleaseItem{},
		[]*opendiscogsmodel.Master{},
		[]*opendiscogsmodel.MasterArtist{},
		[]*opendiscogsmodel.MasterGenre{},
		[]*opendiscogsmodel.MasterStyle{},
		[]*opendiscogsmodel.MasterVideo{},
		[]*opendiscogsmodel.ReleaseItem{},
		[]*opendiscogsmodel.ReleaseItemArtist{},
		[]*opendiscogsmodel.ReleaseItemWork{},
		[]*opendiscogsmodel.ReleaseItemFormat{},
		[]*opendiscogsmodel.ReleaseItemCreditedArtist{},
		[]*opendiscogsmodel.ReleaseItemGenre{},
		[]*opendiscogsmodel.ReleaseItemStyle{},
		[]*opendiscogsmodel.ReleaseItemIdentifier{},
		[]*opendiscogsmodel.ReleaseItemImage{},
		[]*opendiscogsmodel.ReleaseItemTrack{},
		[]*opendiscogsmodel.ReleaseItemVideo{},
		[]*opendiscogsmodel.Style{},
		[]*opendiscogsmodel.Genre{},
		struct{}{},
	)
	require.NoError(t, result.Err())
	require.Zero(t, result.Count())
}

func TestExtractClauseRemainingTypes(t *testing.T) {
	for _, item := range []interface{}{
		&opendiscogsmodel.LabelReleaseItem{},
		&opendiscogsmodel.ReleaseItemFormat{},
		&opendiscogsmodel.LabelSubLabel{},
		&opendiscogsmodel.ReleaseItemImage{},
		&opendiscogsmodel.Style{},
		struct{}{},
	} {
		_ = ExtractClause(item)
	}
}

func TestRegisterCacheBoundaries(t *testing.T) {
	cache.ResetIDs()
	t.Cleanup(cache.ResetIDs)
	for _, item := range []interface{}{
		nil,
		&opendiscogsmodel.Artist{ID: 1},
		&opendiscogsmodel.Label{ID: 2},
		&opendiscogsmodel.Master{ID: 3},
		struct{}{},
	} {
		registerCache(item)
	}
	require.True(t, cache.ArtistIDs.Contains(1))
	require.True(t, cache.LabelIDs.Contains(2))
	require.True(t, cache.MasterIDs.Contains(3))
}

func TestOrderValidationAndCancellation(t *testing.T) {
	require.Panics(t, func() { NewTrackedOrder(context.Background(), 1, 1, "", nil, 0, "artist", false) })
	require.Panics(t, func() { NewTrackedOrder(context.Background(), 1, 1, "", nil, 1, "", false) })
	require.Panics(t, func() { newOrder(context.Background(), 0, 1, "", nil, 0, "", false) })
	require.Panics(t, func() { newOrder(context.Background(), 1, 0, "", nil, 0, "", false) })

	order := newOrder(nil, 1, 1, "", nil, 0, "", false).(*orderImpl)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, order.submitWorker(ctx, func() { t.Fatal("must not run") }))

	order.workers <- struct{}{}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.False(t, order.submitWorker(ctx, func() { t.Fatal("must not run") }))
	<-order.workers

	done := make(chan struct{})
	require.True(t, order.submitWorker(nil, func() { close(done) }))
	<-done
}

func TestSourceErrorRecorder(t *testing.T) {
	recorder := new(sourceErrorRecorder)
	recorder.record(nil)
	require.NoError(t, recorder.get())
	first := errors.New("first")
	recorder.record(first)
	recorder.record(errors.New("second"))
	require.ErrorIs(t, recorder.get(), first)
}

func TestNewReadCloserErrors(t *testing.T) {
	_, err := newReadCloser(filepath.Join(t.TempDir(), "missing.gz"), "fixture")
	require.ErrorContains(t, err, "open import file")

	invalid := filepath.Join(t.TempDir(), "invalid.gz")
	require.NoError(t, os.WriteFile(invalid, []byte("not gzip"), 0600))
	_, err = newReadCloser(invalid, "fixture")
	require.ErrorContains(t, err, "open gzip import file")
}

type writerStub struct {
	result result.Result
}

func (w writerStub) Write(int, ...interface{}) result.Result { return w.result }

type rejectingOrder struct {
	Order
}

func (o rejectingOrder) submitWorker(context.Context, func()) bool { return false }

type untrackedResumeOrder struct {
	Order
}

func (o untrackedResumeOrder) shouldResumeProgress() bool { return true }

func TestSimpleAndRelationPipelineErrorBoundaries(t *testing.T) {
	missingOrder := NewOrder(context.Background(), 1, 1, filepath.Join(t.TempDir(), "missing.gz"), nil)
	require.Error(t, GetArtistStep(missingOrder)().Err())
	require.Error(t, GetLabelStep(missingOrder)().Err())
	require.Error(t, InsertSimple(
		missingOrder,
		"artist",
		"artist",
		(*XmlArtist).TransformAt,
	).Err())
	require.Error(t, processRelationChunks[XmlArtistRelation](
		missingOrder,
		"artist",
		"artist",
		"fixture",
		func(Order, ChunkMetadata, []*XmlArtistRelation) result.Result {
			return result.NewResult(0, nil)
		},
	).Err())

	expected := errors.New("fixture")
	require.ErrorIs(t, simpleInsertResult("artist", rxgo.Error(expected)).Err(), expected)
	require.Error(t, simpleInsertResult("artist", rxgo.Of("invalid")).Err())
	require.NoError(t, simpleInsertResult("artist", rxgo.Of(2)).Err())
}

func TestEntityStepRelationFailures(t *testing.T) {
	expected := errors.New("fixture")
	db := poisonedGorm(t, expected)
	originalWriter := NewWriter
	t.Cleanup(func() { NewWriter = originalWriter })
	NewWriter = func(*gorm.DB) Writer { return writerStub{result: result.NewResult(1, nil)} }

	artistOrder := NewOrder(context.Background(), 2, 1, "testdata/artist.xml.gz", db)
	require.ErrorContains(t, GetArtistStep(artistOrder)().Err(), expected.Error())
	labelOrder := NewOrder(context.Background(), 2, 1, "testdata/label.xml.gz", db)
	require.ErrorContains(t, GetLabelStep(labelOrder)().Err(), expected.Error())
	require.ErrorContains(t, InsertMasterRelations(NewOrder(
		context.Background(), 1, 1, "testdata/master.xml.gz", db,
	)).Err(), expected.Error())
	require.ErrorContains(t, insertReleases(NewOrder(
		context.Background(), 1, 1, "testdata/release.xml.gz", db,
	)).Err(), expected.Error())
}

func TestProcessRelationChunksWorkerAndProgressFailures(t *testing.T) {
	base := NewOrder(context.Background(), 1, 1, "testdata/artist.xml.gz", nil)
	resultValue := processRelationChunks[XmlArtistRelation](
		rejectingOrder{Order: base},
		"artist",
		"artist",
		"fixture",
		func(Order, ChunkMetadata, []*XmlArtistRelation) result.Result {
			return result.NewResult(0, nil)
		},
	)
	require.NoError(t, resultValue.Err())

	expected := errors.New("fixture")
	db := poisonedGorm(t, expected)
	tracked := NewTrackedOrder(context.Background(), 100, 1, "testdata/artist.xml.gz", db, 1, "artist", false)
	resultValue = processRelationChunks[XmlArtistRelation](
		tracked,
		"artist",
		"artist",
		"fixture",
		func(Order, ChunkMetadata, []*XmlArtistRelation) result.Result {
			return result.NewResult(0, nil)
		},
	)
	require.ErrorIs(t, resultValue.Err(), expected)

	require.NoError(t, mergeSourceError(result.NewResult(1, nil), nil).Err())
	require.ErrorIs(t, mergeSourceError(result.NewResult(1, nil), expected).Err(), expected)
}

func TestRelationPipelineReportsPreloadedProgressFailuresWithoutStartingWorkers(t *testing.T) {
	t.Run("inventory query", func(t *testing.T) {
		expected := errors.New("fixture")
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("select chunk_index, first_item_index, item_count").
			WithArgs(int64(0), "").
			WillReturnError(expected)
		order := untrackedResumeOrder{Order: NewOrder(
			context.Background(), 1, 1, "testdata/artist.xml.gz", db,
		)}
		actual := processRelationChunks[XmlArtistRelation](
			order, "artist", "artist", "fixture",
			func(Order, ChunkMetadata, []*XmlArtistRelation) result.Result {
				t.Fatal("writer must not start")
				return nil
			},
		)
		require.ErrorIs(t, actual.Err(), expected)
	})

	t.Run("source range mismatch", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("select chunk_index, first_item_index, item_count").
			WithArgs(int64(0), "").
			WillReturnRows(sqlmock.NewRows(
				[]string{"chunk_index", "first_item_index", "item_count"},
			).AddRow(0, 99, 1))
		order := untrackedResumeOrder{Order: NewOrder(
			context.Background(), 1, 1, "testdata/artist.xml.gz", db,
		)}
		actual := processRelationChunks[XmlArtistRelation](
			order, "artist", "artist", "fixture",
			func(Order, ChunkMetadata, []*XmlArtistRelation) result.Result {
				t.Fatal("writer must not start")
				return nil
			},
		)
		require.ErrorContains(t, actual.Err(), "does not match source range")
	})
}

func TestReconcileRelationsStopsAtFirstFailure(t *testing.T) {
	expected := errors.New("fixture")
	thirdCalled := false
	actual := reconcileRelations([]func() result.Result{
		func() result.Result { return result.NewResult(2, nil) },
		func() result.Result { return result.NewResult(3, expected) },
		func() result.Result {
			thirdCalled = true
			return result.NewResult(5, nil)
		},
	})
	require.Equal(t, 5, actual.Count())
	require.ErrorIs(t, actual.Err(), expected)
	require.False(t, thirdCalled)
}

func TestWriteRelationChunkControlFlow(t *testing.T) {
	originalWriter := NewWriter
	t.Cleanup(func() { NewWriter = originalWriter })

	run := func(
		t *testing.T,
		wantError bool,
		prepare func(sqlmock.Sqlmock),
		write func(Order) result.Result,
	) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		if prepare != nil {
			prepare(mock)
		}
		if wantError {
			mock.ExpectRollback()
		} else {
			mock.ExpectCommit()
		}
		actual := write(NewOrder(context.Background(), 1, 1, "unused", db))
		require.Equal(t, wantError, actual.IsErr())
	}

	NewWriter = func(*gorm.DB) Writer {
		t.Fatal("Artist and Label relation passes must not write root records")
		return nil
	}
	run(t, false, nil, func(order Order) result.Result {
		return writeArtistRelationChunk(order, ChunkMetadata{}, []*XmlArtistRelation{nil})
	})
	run(t, false, nil, func(order Order) result.Result {
		return writeLabelRelationChunk(order, ChunkMetadata{}, []*XmlLabelRelation{nil})
	})
	NewWriter = func(*gorm.DB) Writer { return writerStub{result: result.NewResult(0, nil)} }
	run(t, false, nil, func(order Order) result.Result {
		return writeMasterRelationChunk(order, ChunkMetadata{}, []*XmlMasterRelation{nil})
	})
	run(t, false, nil, func(order Order) result.Result {
		return writeReleaseRelationChunk(order, ChunkMetadata{}, []*XmlReleaseRelation{nil})
	})

	NewWriter = func(*gorm.DB) Writer { return writerStub{result: result.NewResult(0, errors.New("writer"))} }
	run(t, true, func(mock sqlmock.Sqlmock) {
		expectExistingRelationRoots(mock, masterArtistRelation.table)
	}, func(order Order) result.Result {
		return writeMasterRelationChunk(order, ChunkMetadata{}, []*XmlMasterRelation{{ID: 1}})
	})
	run(t, true, func(mock sqlmock.Sqlmock) {
		expectExistingRelationRoots(mock, releaseArtistRelation.table)
	}, func(order Order) result.Result {
		return writeReleaseRelationChunk(order, ChunkMetadata{}, []*XmlReleaseRelation{{ID: 1}})
	})

	NewWriter = func(*gorm.DB) Writer { return writerStub{result: result.NewResult(0, nil)} }
	run(t, true, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("select '" + artistAliasRelation.table + "'").WillReturnError(errors.New("roots"))
	}, func(order Order) result.Result {
		return writeArtistRelationChunk(order, ChunkMetadata{}, []*XmlArtistRelation{{ID: 1}})
	})
	run(t, true, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("select '" + labelURLRelation.table + "'").WillReturnError(errors.New("roots"))
	}, func(order Order) result.Result {
		return writeLabelRelationChunk(order, ChunkMetadata{}, []*XmlLabelRelation{{ID: 1}})
	})
	run(t, true, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("select '" + masterArtistRelation.table + "'").WillReturnError(errors.New("roots"))
	}, func(order Order) result.Result {
		return writeMasterRelationChunk(order, ChunkMetadata{}, []*XmlMasterRelation{{ID: 1}})
	})
	run(t, true, func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("select '" + releaseArtistRelation.table + "'").WillReturnError(errors.New("roots"))
	}, func(order Order) result.Result {
		return writeReleaseRelationChunk(order, ChunkMetadata{}, []*XmlReleaseRelation{{ID: 1}})
	})
}

func TestWriterErrorControlFlow(t *testing.T) {
	expected := errors.New("fixture")
	db := poisonedGorm(t, expected)
	actual := gormWriter{db: db}.Write(
		1,
		[]*opendiscogsmodel.Genre{{Name: "genre"}},
		[]*opendiscogsmodel.Style{{Name: "style"}},
	)
	require.ErrorIs(t, actual.Err(), expected)

	mockDB, _, _ := newMockGorm(t)
	type invalidSchema struct {
		Channel chan int
	}
	require.ErrorContains(t, doWrite([]invalidSchema{{}}, 1, mockDB).Err(), "parse insert schema")
	require.ErrorContains(t, doWrite([]*opendiscogsmodel.Artist{{ID: 1}}, 0, mockDB).Err(), "chunk size")
}

func TestReferenceEntitiesUseDeterministicLockOrder(t *testing.T) {
	db, mock, _ := newMockGorm(t)
	mock.ExpectExec(`INSERT INTO "genre"`).
		WithArgs("Ambient", "Electronic").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`INSERT INTO "style"`).
		WithArgs("Dub", "Techno").
		WillReturnResult(sqlmock.NewResult(0, 2))
	order := NewOrder(context.Background(), 10, 1, "unused", db)
	genres := []*opendiscogsmodel.Genre{{Name: "Electronic"}, {Name: "Ambient"}}
	styles := []*opendiscogsmodel.Style{{Name: "Techno"}, {Name: "Dub"}}
	sortReferenceEntities(genres, styles)

	actual := writeReferenceEntities(
		order,
		genres,
		styles,
	)

	require.NoError(t, actual.Err())
}

func TestConfirmedReferenceEntitiesAreNotSentToPostgreSQLAgain(t *testing.T) {
	cache.ResetIDs()
	t.Cleanup(cache.ResetIDs)
	confirmReferenceEntities(
		[]*opendiscogsmodel.Genre{{Name: "Electronic"}},
		[]*opendiscogsmodel.Style{{Name: "Techno"}},
	)

	genres, styles := filterConfirmedReferenceEntities(
		[]*opendiscogsmodel.Genre{{Name: "Ambient"}, {Name: "Electronic"}},
		[]*opendiscogsmodel.Style{{Name: "Dub"}, {Name: "Techno"}},
	)

	require.Equal(t, []string{"Ambient"}, []string{genres[0].Name})
	require.Equal(t, []string{"Dub"}, []string{styles[0].Name})
}

func TestReleaseUpdateAndReferenceFilters(t *testing.T) {
	expected := errors.New("fixture")
	db, mock, _ := newMockGorm(t)
	mock.ExpectBegin()
	mock.ExpectExec("WITH desired").WillReturnError(expected)
	mock.ExpectRollback()
	actual := reconcileMasterMainReleases(
		NewOrder(context.Background(), 1, 1, "unused", db),
	)
	require.ErrorIs(t, actual.Err(), expected)
	require.Empty(t, filterGenres([]*opendiscogsmodel.Genre{nil, {Name: " "}}))
	require.Empty(t, filterStyles([]*opendiscogsmodel.Style{nil, {Name: " "}}))

	db, mock, _ = newMockGorm(t)
	mock.ExpectBegin()
	mock.ExpectExec("WITH desired").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("WITH desired").
		WillReturnError(expected)
	mock.ExpectRollback()
	actual = reconcileMasterMainReleases(
		NewOrder(context.Background(), 1, 1, "unused", db),
	)
	require.ErrorIs(t, actual.Err(), expected)

	db, mock, _ = newMockGorm(t)
	mock.ExpectBegin()
	mock.ExpectExec("WITH desired").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("WITH desired").
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()
	actual = finalizeReleaseImport(
		NewOrder(context.Background(), 1, 1, "unused", db),
		5,
		2,
	)
	require.NoError(t, actual.Err())
	require.Equal(t, 5, actual.Count())
}

func TestReleaseMasterReconciliationUsesFixedSetStatements(t *testing.T) {
	require.Contains(t, releaseMainReleaseClearSQL, "FOR UPDATE OF target")
	require.Contains(t, releaseMainReleaseSetSQL, "FOR UPDATE OF target")
	require.Contains(t, releaseMainReleaseClearSQL, "main_release_id = NULL")
	require.Contains(t, releaseMainReleaseSetSQL, "main_release_id = pending.release_id")
	require.NotContains(t, releaseMainReleaseClearSQL, "VALUES")
	require.NotContains(t, releaseMainReleaseSetSQL, "VALUES")
}

func TestReferenceWriteFailuresInRelationChunks(t *testing.T) {
	cache.ResetIDs()
	t.Cleanup(cache.ResetIDs)
	originalWriter := NewWriter
	t.Cleanup(func() { NewWriter = originalWriter })
	NewWriter = func(*gorm.DB) Writer { return writerStub{result: result.NewResult(0, nil)} }
	expected := errors.New("fixture")

	writes := []struct {
		table string
		write func(Order) result.Result
	}{
		{table: masterArtistRelation.table, write: func(order Order) result.Result {
			return writeMasterRelationChunk(order, ChunkMetadata{}, []*XmlMasterRelation{{
				ID: 1, Genres: []string{"genre"},
			}})
		}},
		{table: releaseArtistRelation.table, write: func(order Order) result.Result {
			return writeReleaseRelationChunk(order, ChunkMetadata{}, []*XmlReleaseRelation{{
				ID: 1, Genres: []string{"genre"},
			}})
		}},
	}
	for _, fixture := range writes {
		db, mock, _ := newMockGorm(t)
		mock.ExpectBegin()
		expectExistingRelationRoots(mock, fixture.table)
		mock.ExpectExec(`INSERT INTO .*genre`).WithArgs("genre").WillReturnError(expected)
		mock.ExpectRollback()
		require.ErrorIs(t, fixture.write(NewOrder(context.Background(), 1, 1, "unused", db)).Err(), expected)
		require.False(t, cache.GenreNames.Contains("genre"))
	}
}

func TestLabelSubLabelReconcileAccessors(t *testing.T) {
	expected := errors.New("fixture")
	db := poisonedGorm(t, expected)
	order := NewOrder(context.Background(), 1, 1, "unused", db)
	actual := reconcileIntegerRelation(
		order,
		labelSubLabelRelation,
		true,
		[]int32{1},
		[]*opendiscogsmodel.LabelSubLabel{{ParentLabelID: 1, SubLabelID: 2}},
		func(item *opendiscogsmodel.LabelSubLabel) int32 { return item.ParentLabelID },
		func(item *opendiscogsmodel.LabelSubLabel) int32 { return item.SubLabelID },
	)
	require.ErrorIs(t, actual.Err(), expected)
}

func TestLabelChunkUsesSubLabelKeys(t *testing.T) {
	originalWriter := NewWriter
	t.Cleanup(func() { NewWriter = originalWriter })
	NewWriter = func(*gorm.DB) Writer { return writerStub{result: result.NewResult(0, nil)} }
	cache.LabelIDs.Add(2)
	t.Cleanup(cache.ResetIDs)
	db, mock, _ := newMockGorm(t)
	mock.ExpectBegin()
	expectExistingRelationRoots(
		mock,
		labelURLRelation.table,
		existingRelationRootFixture{table: labelURLRelation.table, rootID: 1},
		existingRelationRootFixture{table: labelSubLabelRelation.table, rootID: 1},
	)
	mock.ExpectExec("delete from label_url").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("delete from label_sub_label").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(errors.New("fixture"))
	mock.ExpectRollback()

	actual := writeLabelRelationChunk(
		NewOrder(context.Background(), 1, 1, "unused", db),
		ChunkMetadata{},
		[]*XmlLabelRelation{{ID: 1, SubLabels: []XmlRef{{ID: 2}}}},
	)
	require.Error(t, actual.Err())
}

func TestLabelRelationBuildsCanonicalRoot(t *testing.T) {
	name := "Label"
	label := (&XmlLabelRelation{ID: 7, Name: &name}).GetLabel()
	require.Equal(t, int32(7), label.ID)
	require.Equal(t, name, *label.Name)
	require.False(t, label.CreatedAt.IsZero())
	require.Equal(t, label.CreatedAt, label.LastModifiedAt)
}

func createGzipFixture(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.xml.gz")
	file, err := os.Create(path)
	require.NoError(t, err)
	compressed := gzip.NewWriter(file)
	_, err = compressed.Write([]byte(body))
	require.NoError(t, err)
	require.NoError(t, compressed.Close())
	require.NoError(t, file.Close())
	return path
}
