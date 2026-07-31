package batch

import (
	"context"
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"io"
	"os"
	"sync"
	"time"
)

func GetArtistStep(order Order) Step {
	return func() result.Result {
		updated := 0
		res := insertArtists(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		res = insertArtistRelations(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		return result.NewResult(updated, nil)
	}
}

func insertArtists(order Order) result.Result {
	return InsertSimple[XmlArtist, model.Artist](order, "artists", "artist")
}

func insertArtistRelations(order Order) result.Result {
	r := newReadCloser(order.getFilePath(), "updating artist relations...")

	var (
		wg   = new(sync.WaitGroup)
		res  = make(chan result.Result)
		done = make(chan struct{}, 1)
	)

	go func() {
		<-reader.NewReader[XmlArtistRelation](context.Background(), r, "artist").
			WindowWithCount(order.getChunkSize()).
			Map(helper.MapWindowedSlice[*XmlArtistRelation]()).
			ForEach(
				writeArtistRelations(order, res, wg),
				printError(),
				signalDone(done, wg))
	}()

	go func() { // wait until done called then close res chan
		<-done
		close(res)
	}()

	sum := result.NewResult(0, nil)
	for next := range res {
		if next == nil {
			continue
		}
		sum = sum.Sum(next)
	}

	fmt.Printf("\nUpdated %+v artist relations\n", sum.Count())
	return sum
}

func writeArtistRelations(order Order, res chan result.Result, wg *sync.WaitGroup) func(i interface{}) {
	return func(i interface{}) {
		wg.Add(1)
		items := i.([]*XmlArtistRelation)
		n := make([]*model.ArtistNameVariation, 0)
		a := make([]*model.ArtistAlias, 0)
		g := make([]*model.ArtistGroup, 0)
		m := make([]*model.ArtistMember, 0)
		u := make([]*model.ArtistURL, 0)
		for _, item := range items {
			a = append(a, item.GetAliases()...)
			g = append(g, item.GetGroups()...)
			m = append(m, item.GetMembers()...)
			n = append(n, item.GetNameVars()...)
			u = append(u, item.GetUrls()...)
		}
		go func(res chan result.Result) {
			defer wg.Done()
			res <- writeThenReport(order, wg, a, g, m, n, u)
		}(res)
	}
}

func printError() func(err error) {
	return func(err error) {
		fmt.Printf("[ERROR] %+v %+v\n", time.Now().Format(time.Layout), err)
	}
}

func newReadCloser(filepath string, progressBarText string) io.ReadCloser {
	if f, err := os.Open(filepath); err != nil {
		panic(err)
	} else if r, err := reader.NewProgressBarGzipReadCloser(f, progressBarText); err != nil {
		_ = f.Close()
		panic(r)
	} else {
		return r
	}
}

func insertBySlice[T any](order Order) func(_ context.Context, i interface{}) (interface{}, error) {
	return func(_ context.Context, i interface{}) (interface{}, error) {
		res := NewWriter(order.getDB()).Write(order.getChunkSize(), i.([]T))
		return res.Count(), res.Err()
	}
}
