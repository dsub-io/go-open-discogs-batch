package batch

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func writeChunk(order Order, slices ...interface{}) result.Result {
	return NewWriter(order.getDB()).Write(order.getChunkSize(), slices...)
}

func writeReferenceEntities(
	order Order,
	genres []*model.Genre,
	styles []*model.Style,
) result.Result {
	for _, items := range []interface{}{genres, styles} {
		if err := order.getDB().
			Clauses(clause.OnConflict{DoNothing: true}).
			CreateInBatches(items, order.getChunkSize()).Error; err != nil {
			return result.NewResult(0, err)
		}
	}
	return result.NewResult(0, nil)
}

func sortReferenceEntities(genres []*model.Genre, styles []*model.Style) {
	slices.SortFunc(genres, func(left, right *model.Genre) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(styles, func(left, right *model.Style) int {
		return strings.Compare(left.Name, right.Name)
	})
}

type Writer interface {
	Write(chunkSize int, items ...interface{}) result.Result
}

type gormWriter struct {
	db *gorm.DB
}

var NewWriter = newWriter

func newWriter(db *gorm.DB) Writer {
	return &gormWriter{db: db}
}

func (g gormWriter) Write(chunkSize int, slices ...interface{}) result.Result {
	var (
		updated = 0
		err     error
	)
	err = nil
	for _, slice := range slices {
		if err != nil {
			break
		}
		var r result.Result
		switch o := slice.(type) {
		case []*model.Artist:
			r = doWrite[*model.Artist](o, chunkSize, g.db)
		case []*model.ArtistURL:
			r = doWrite[*model.ArtistURL](o, chunkSize, g.db)
		case []*model.ArtistAlias:
			r = doWrite[*model.ArtistAlias](o, chunkSize, g.db)
		case []*model.ArtistGroup:
			r = doWrite[*model.ArtistGroup](o, chunkSize, g.db)
		case []*model.ArtistMember:
			r = doWrite[*model.ArtistMember](o, chunkSize, g.db)
		case []*model.ArtistNameVariation:
			r = doWrite[*model.ArtistNameVariation](o, chunkSize, g.db)
		case []*model.Label:
			r = doWrite[*model.Label](o, chunkSize, g.db)
		case []*model.LabelURL:
			r = doWrite[*model.LabelURL](o, chunkSize, g.db)
		case []*model.LabelSubLabel:
			r = doWrite[*model.LabelSubLabel](o, chunkSize, g.db)
		case []*model.LabelReleaseItem:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateLabelReleaseItems)
		case []*model.Master:
			r = doWrite[*model.Master](o, chunkSize, g.db)
		case []*model.MasterArtist:
			r = doWrite[*model.MasterArtist](o, chunkSize, g.db)
		case []*model.MasterGenre:
			r = doWrite[*model.MasterGenre](o, chunkSize, g.db)
		case []*model.MasterStyle:
			r = doWrite[*model.MasterStyle](o, chunkSize, g.db)
		case []*model.MasterVideo:
			r = doWrite[*model.MasterVideo](o, chunkSize, g.db)
		case []*model.ReleaseItem:
			r = doWrite[*model.ReleaseItem](o, chunkSize, g.db)
		case []*model.ReleaseItemArtist:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseArtists)
		case []*model.ReleaseItemWork:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseWorks)
		case []*model.ReleaseItemFormat:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseFormats)
		case []*model.ReleaseItemCreditedArtist:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseCreditedArtists)
		case []*model.ReleaseItemGenre:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseGenres)
		case []*model.ReleaseItemStyle:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseStyles)
		case []*model.ReleaseItemIdentifier:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseIdentifiers)
		case []*model.ReleaseItemImage:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseImages)
		case []*model.ReleaseItemTrack:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseTracks)
		case []*model.ReleaseItemVideo:
			r = writeReleaseRelationBatch(o, chunkSize, g.db, deduplicateReleaseVideos)
		case []*model.Style:
			r = doWrite[*model.Style](o, chunkSize, g.db)
		case []*model.Genre:
			r = doWrite[*model.Genre](o, chunkSize, g.db)
		}
		if r != nil {
			updated += r.Count()
			err = r.Err()
		}
	}

	return result.NewResult(updated, err)
}

func writeReleaseRelationBatch[T any](
	items []T,
	chunkSize int,
	db *gorm.DB,
	deduplicate func([]T) ([]T, error),
) result.Result {
	deduplicated, err := deduplicate(items)
	if err != nil {
		return result.NewResult(0, err)
	}
	return doWrite(deduplicated, chunkSize, db)
}

func doWrite[T any](items []T, chunkSize int, db *gorm.DB) result.Result {
	if len(items) == 0 {
		return result.NewResult(0, nil)
	}
	statement := &gorm.Statement{DB: db}
	if err := statement.Parse(items[0]); err != nil {
		return result.NewResult(0, fmt.Errorf("parse insert schema: %w", err))
	}
	chunkSize, err := postgresSafeBatchSize(chunkSize, len(statement.Schema.DBNames))
	if err != nil {
		return result.NewResult(0, err)
	}
	var (
		start     = 0
		end       = chunkSize
		resultSum = result.NewResult(0, nil)
		size      = len(items)
		cl        = ExtractClause(items[0])
	)
	for {
		if resultSum.IsErr() {
			logrus.Errorf("error during insertion: %+v\n", resultSum.Err())
		}
		if start >= size || resultSum.Err() != nil {
			return resultSum
		}
		if end > size {
			end = size
		}
		part := unique.Slice(items[start:end])
		tx := db.Clauses(cl).CreateInBatches(&part, len(part))
		resultSum = resultSum.Sum(result.NewResult(int(tx.RowsAffected), tx.Error))
		start += chunkSize
		end += chunkSize
	}
}

func postgresSafeBatchSize(requested, columnCount int) (int, error) {
	const postgresBindParameterLimit = 65_535
	if requested < 1 {
		return 0, fmt.Errorf("chunk size must be positive")
	}
	if columnCount < 1 {
		return 0, fmt.Errorf("insert schema has no columns")
	}
	maxRows := postgresBindParameterLimit / columnCount
	if maxRows < 1 {
		return 0, fmt.Errorf("insert schema has too many columns: %d", columnCount)
	}
	return min(requested, maxRows), nil
}
