package batch

import (
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"gorm.io/gorm"
)

type Step func() result.Result

type Batch interface {
	UpdateArtist(order Order) Step
	UpdateLabel(order Order) Step
	UpdateMaster(order Order) Step
	UpdateRelease(order Order) Step
}

type batch struct {
	db *gorm.DB
}

func New() Batch {
	return newBatch()
}

var newBatch = func() Batch {
	return &batch{database.DB}
}

func (b *batch) UpdateArtist(order Order) Step {
	return GetArtistStep(order)
}

func (b *batch) UpdateLabel(order Order) Step {
	return GetLabelStep(order)
}

func (b *batch) UpdateMaster(order Order) Step {
	return GetMasterStep(order)
}

func (b *batch) UpdateRelease(order Order) Step {
	return GetReleaseStep(order)
}
