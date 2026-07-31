package data

import (
	"errors"
	"fmt"

	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
	"time"
)

type Repository interface {
	BatchInsert([]*Data) (int, error)
	FindByYearMonthType(year, month, typ string) (*opendiscogsmodel.DiscogsDump, error)
}

type repositoryImpl struct {
	DB *gorm.DB
}

func (d *repositoryImpl) BatchInsert(items []*Data) (int, error) {
	dumps := make([]*opendiscogsmodel.DiscogsDump, 0, len(items))
	for _, item := range items {
		if item.TargetType == "checksum" {
			continue
		}
		dumps = append(dumps, item.Dump())
	}
	if len(dumps) == 0 {
		return 0, nil
	}
	tx := d.DB.
		Omit("ID", "CreatedAt", "LastModifiedAt").
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "dump_date"},
				{Name: "entity_type"},
				{Name: "checksum_sha256"},
			},
			DoNothing: true,
		}).
		CreateInBatches(&dumps, len(dumps))
	return int(tx.RowsAffected), tx.Error
}

func (d *repositoryImpl) FindByYearMonthType(
	y, m, t string,
) (*opendiscogsmodel.DiscogsDump, error) {
	var (
		result opendiscogsmodel.DiscogsDump
		begin  time.Time
		end    time.Time
		err    error
	)

	if begin, err = time.Parse("20060102", y+m+"01"); err != nil {
		return nil, errors.New("failed to parse y and m: " + y + "." + m)
	}
	end = begin.AddDate(0, 1, 0)
	entityType := strings.TrimSuffix(strings.ToLower(t), "s")
	tx := d.DB.
		Where("entity_type = ? AND dump_date >= ? AND dump_date < ?", entityType, begin, end).
		Order("dump_date DESC, id DESC").
		First(&result)
	err = tx.Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = fmt.Errorf("%s data not found from y:%s m:%s", t, y, m)
	}
	return &result, err
}

func NewDataRepository(db *gorm.DB) Repository {
	return &repositoryImpl{db}
}
