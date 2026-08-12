package batch

import (
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"slices"
	"strings"
)

func writeChunk(order Order, slices ...interface{}) result.Result {
	return NewWriter(order.getDB()).Write(order.getChunkSize(), slices...)
}

func writeReferenceEntities(
	order Order,
	genres []*model.Genre,
	styles []*model.Style,
) result.Result {
	slices.SortFunc(genres, func(left, right *model.Genre) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(styles, func(left, right *model.Style) int {
		return strings.Compare(left.Name, right.Name)
	})
	for _, items := range []interface{}{genres, styles} {
		if err := order.getDB().
			Clauses(clause.OnConflict{DoNothing: true}).
			CreateInBatches(items, order.getChunkSize()).Error; err != nil {
			return result.NewResult(0, err)
		}
	}
	return result.NewResult(0, nil)
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
			r = doWrite[*model.LabelReleaseItem](o, chunkSize, g.db)
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
			r = doWrite[*model.ReleaseItemArtist](o, chunkSize, g.db)
		case []*model.ReleaseItemWork:
			r = doWrite[*model.ReleaseItemWork](o, chunkSize, g.db)
		case []*model.ReleaseItemFormat:
			r = doWrite[*model.ReleaseItemFormat](o, chunkSize, g.db)
		case []*model.ReleaseItemCreditedArtist:
			r = doWrite[*model.ReleaseItemCreditedArtist](o, chunkSize, g.db)
		case []*model.ReleaseItemGenre:
			r = doWrite[*model.ReleaseItemGenre](o, chunkSize, g.db)
		case []*model.ReleaseItemStyle:
			r = doWrite[*model.ReleaseItemStyle](o, chunkSize, g.db)
		case []*model.ReleaseItemIdentifier:
			r = doWrite[*model.ReleaseItemIdentifier](o, chunkSize, g.db)
		case []*model.ReleaseItemImage:
			r = doWrite[*model.ReleaseItemImage](o, chunkSize, g.db)
		case []*model.ReleaseItemTrack:
			r = doWrite[*model.ReleaseItemTrack](o, chunkSize, g.db)
		case []*model.ReleaseItemVideo:
			r = doWrite[*model.ReleaseItemVideo](o, chunkSize, g.db)
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

func doWrite[T comparable](items []T, chunkSize int, db *gorm.DB) result.Result {
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
