package batch

import (
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
	"github.com/dsub-io/open-discogs-model/model"
	"gorm.io/gorm/clause"
	"strings"
	"sync"
	"time"
)

// TODO: double check ptr exceptions on reference

// GetReleaseStep returns a set of steps in a form of composite notary.
// This is a convenient func such that reduces code and adds syntactic sugar, but nothing more.
func GetReleaseStep(order Order) Step {
	return func() result.Result {
		return insertReleases(order)
	}
}

func insertReleases(order Order) result.Result {
	r := newReadCloser(order.getFilePath(), "updating releases...")

	var (
		wg   = new(sync.WaitGroup)
		res  = make(chan result.Result)
		done = make(chan struct{}, 1)
	)

	go func() {
		<-reader.NewReader[XmlReleaseRelation](order.getContext(), r, "release").
			WindowWithCount(order.getChunkSize()).
			Map(helper.MapWindowedSlice[*XmlReleaseRelation]()).
			ForEach(
				doInsertReleases(order, res, wg),
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

	fmt.Printf("\nUpdated %+v release relations\n", sum.Count())
	return sum
}

func doInsertReleases(order Order, res chan result.Result, wg *sync.WaitGroup) func(i interface{}) {
	return func(i interface{}) {
		wg.Add(1)
		rrs := i.([]*XmlReleaseRelation)

		var (
			g   = make([]*model.Genre, 0)
			s   = make([]*model.Style, 0)
			rel = make([]*model.ReleaseItem, 0)
			ra  = make([]*model.ReleaseItemArtist, 0)
			rca = make([]*model.ReleaseItemCreditedArtist, 0)
			rw  = make([]*model.ReleaseItemWork, 0)
			rf  = make([]*model.ReleaseItemFormat, 0)
			rs  = make([]*model.ReleaseItemStyle, 0)
			rg  = make([]*model.ReleaseItemGenre, 0)
			ri  = make([]*model.ReleaseItemIdentifier, 0)
			rt  = make([]*model.ReleaseItemTrack, 0)
			rv  = make([]*model.ReleaseItemVideo, 0)
			rl  = make([]*model.LabelReleaseItem, 0)
		)

		for _, rr := range rrs {
			if rr == nil {
				continue
			}
			g = append(g, rr.GetGenres()...)
			s = append(s, rr.GetStyles()...)
		}

		order.getDB().
			Clauses(clause.OnConflict{DoNothing: true}).
			CreateInBatches(filterGenres(g), order.getChunkSize())
		order.getDB().
			Clauses(clause.OnConflict{DoNothing: true}).
			CreateInBatches(filterStyles(s), order.getChunkSize())

		for _, rr := range rrs {
			if rr == nil {
				continue
			}
			rel = append(rel, rr.GetRelease())
			ra = append(ra, rr.GetReleaseArtists()...)
			rg = append(rg, rr.GetReleaseGenres()...)
			rs = append(rs, rr.GetReleaseStyles()...)
			rw = append(rw, rr.GetWorks()...)
			rl = append(rl, rr.GetLabels()...)
			rf = append(rf, rr.GetFormats()...)
			ri = append(ri, rr.GetIdentifiers()...)
			rt = append(rt, rr.GetTracks()...)
			rv = append(rv, rr.GetVideos()...)
			rca = append(rca, rr.GetCreditedArtists()...)
		}

		go func(res chan result.Result) {
			defer wg.Done()
			written := writeThenReport(order, wg, rel, ra, rw, rs, rg, rl, rf, ri, rt, rv, rca)
			if !written.IsErr() {
				written = written.Sum(updateMasterMainReleases(order, rrs))
			}
			res <- written
		}(res)
	}
}

func updateMasterMainReleases(order Order, releases []*XmlReleaseRelation) result.Result {
	updated := 0
	for _, release := range releases {
		if release == nil || !release.MasterInfo.IsMaster || release.MasterInfo.MasterID == nil {
			continue
		}
		tx := order.getDB().
			Model(&model.Master{}).
			Where("id = ?", *release.MasterInfo.MasterID).
			Where("main_release_id IS DISTINCT FROM ?", release.ID).
			Updates(map[string]any{
				"main_release_id":  release.ID,
				"last_modified_at": time.Now().UTC(),
			})
		if tx.Error != nil {
			return result.NewResult(updated, tx.Error)
		}
		updated += int(tx.RowsAffected)
	}
	return result.NewResult(updated, nil)
}

func filterGenres(genres []*model.Genre) []*model.Genre {
	r := make([]*model.Genre, 0)
	for _, v := range unique.Slice(genres) {
		if name := strings.TrimSpace(v.Name); len(name) == 0 {
			continue
		}
		r = append(r, v)
	}
	return r
}

func filterStyles(styles []*model.Style) []*model.Style {
	r := make([]*model.Style, 0)
	for _, v := range unique.Slice(styles) {
		if name := strings.TrimSpace(v.Name); len(name) == 0 {
			continue
		}
		r = append(r, v)
	}
	return r
}
