package batch

import (
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"gorm.io/gorm/clause"
	"sync"
)

//TODO: add master release step for future use

func GetMasterStep(order Order) Step {
	return func() result.Result {
		return InsertMasterRelations(order)
	}
}

func InsertMasterRelations(order Order) result.Result {
	r := newReadCloser(order.getFilePath(), "updating master relations...")
	var (
		wg   = new(sync.WaitGroup)
		res  = make(chan result.Result)
		done = make(chan struct{}, 1)
	)

	go func() {
		<-reader.NewReader[XmlMasterRelation](order.getContext(), r, "master").
			WindowWithCount(order.getChunkSize()).
			Map(helper.MapWindowedSlice[*XmlMasterRelation]()).
			ForEach(
				WriteMasterRelations(order, res, wg),
				printError(),
				signalDone(done, wg))
	}()

	go func() {
		<-done
		close(res)
	}()

	sum := result.NewResult(0, nil)
	for next := range res {
		sum = sum.Sum(next)
	}

	fmt.Printf("\nUpdated %+v master relations\n", sum.Count())
	return sum
}

func WriteMasterRelations(order Order, res chan result.Result, wg *sync.WaitGroup) func(i interface{}) {
	return func(i interface{}) {
		wg.Add(1) // process takes time, hence add lock scenario
		mrs := i.([]*XmlMasterRelation)

		s := make([]*model.Style, 0)
		g := make([]*model.Genre, 0)

		for _, rr := range mrs {
			if rr == nil {
				continue
			}
			g = append(g, rr.GetGenres()...)
			s = append(s, rr.GetStyles()...)
		}

		s = filterStyles(s)
		g = filterGenres(g)

		order.getDB().
			Clauses(clause.OnConflict{DoNothing: true}).
			CreateInBatches(filterGenres(g), order.getChunkSize())
		order.getDB().
			Clauses(clause.OnConflict{DoNothing: true}).
			CreateInBatches(filterStyles(s), order.getChunkSize())

		var (
			m  = make([]*model.Master, 0)
			mv = make([]*model.MasterVideo, 0)
			ms = make([]*model.MasterStyle, 0)
			mg = make([]*model.MasterGenre, 0)
			ma = make([]*model.MasterArtist, 0)
		)

		for _, mr := range mrs {
			if mr == nil {
				continue
			}
			master := mr.GetMaster()
			m = append(m, master)
			cache.MasterIDCache.Store(master.ID, struct{}{})
			ms = append(ms, mr.GetMasterStyles()...)
			mg = append(mg, mr.GetMasterGenres()...)
			mv = append(mv, mr.GetMasterVideos()...)
			ma = append(ma, mr.GetMasterArtists()...)
		}
		go func(res chan result.Result) {
			defer wg.Done()
			res <- writeThenReport(order, wg, m, mv, ms, mg, ma)
		}(res)
	}
}
