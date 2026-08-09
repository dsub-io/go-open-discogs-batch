package batch

import (
	"strings"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/go-open-discogs-batch/src/unique"
	"github.com/dsub-io/open-discogs-model/model"
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
	return processRelationChunks(
		order,
		"release relations",
		"release",
		"updating releases...",
		writeReleaseRelationChunk,
	)
}

func writeReleaseRelationChunk(order Order, items []*XmlReleaseRelation) result.Result {
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
	for _, item := range items {
		if item == nil {
			continue
		}
		g = append(g, item.GetGenres()...)
		s = append(s, item.GetStyles()...)
	}
	if referenceResult := writeReferenceEntities(order, filterGenres(g), filterStyles(s)); referenceResult.IsErr() {
		return referenceResult
	}
	for _, item := range items {
		if item == nil {
			continue
		}
		rel = append(rel, item.GetRelease())
		ra = append(ra, item.GetReleaseArtists()...)
		rg = append(rg, item.GetReleaseGenres()...)
		rs = append(rs, item.GetReleaseStyles()...)
		rw = append(rw, item.GetWorks()...)
		rl = append(rl, item.GetLabels()...)
		rf = append(rf, item.GetFormats()...)
		ri = append(ri, item.GetIdentifiers()...)
		rt = append(rt, item.GetTracks()...)
		rv = append(rv, item.GetVideos()...)
		rca = append(rca, item.GetCreditedArtists()...)
	}
	written := writeChunk(order, rel, ra, rw, rs, rg, rl, rf, ri, rt, rv, rca)
	if !written.IsErr() {
		written = written.Sum(updateMasterMainReleases(order, items))
	}
	return written
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
