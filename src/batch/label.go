package batch

import (
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/dsub-io/open-discogs-model/model"
	"sync"
)

//TODO: add label release step for future use

func GetLabelStep(order Order) Step {
	return func() result.Result {
		updated := 0
		res := insertLabels(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		res = insertLabelRelations(order)
		updated += res.Count()
		if res.IsErr() {
			return result.NewResult(updated, res.Err())
		}
		return result.NewResult(updated, nil)
	}
}

func insertLabels(order Order) result.Result {
	return InsertSimple[XmlLabel, model.Label](order, "labels", "label")
}

func insertLabelRelations(order Order) result.Result {
	r := newReadCloser(order.getFilePath(), "updating label relations...")

	var (
		wg   = new(sync.WaitGroup)
		res  = make(chan result.Result)
		done = make(chan struct{}, 1)
	)

	go func() {
		<-reader.NewReader[XmlLabelRelation](order.getContext(), r, "label").
			WindowWithCount(order.getChunkSize()).
			Map(helper.MapWindowedSlice[*XmlLabelRelation]()).
			ForEach(
				writeLabelRelations(order, res, wg), // DoOnNext
				printError(),                        // DoOnError
				signalDone(done, wg))                //DoOnComplete
	}()

	go func() { // wait until done called then close res chan
		<-done
		close(res)
	}()

	sum := result.NewResult(0, nil)
	for next := range res {
		sum = sum.Sum(next)
	}

	fmt.Printf("\nUpdated %+v label relations\n", sum.Count())
	return sum
}

func signalDone(done chan<- struct{}, wg *sync.WaitGroup) func() {
	return func() {
		defer close(done)
		wg.Wait()
		done <- struct{}{}
	}
}

func writeLabelRelations(order Order, res chan result.Result, wg *sync.WaitGroup) func(i interface{}) {
	return func(i interface{}) {
		wg.Add(1)
		u := make([]*model.LabelURL, 0)
		s := make([]*model.LabelSubLabel, 0)
		lrs := i.([]*XmlLabelRelation)
		for _, lr := range lrs {
			u = append(u, lr.GetUrls()...)
			s = append(s, lr.GetSubLabels()...)
		}
		go func() {
			defer wg.Done()
			res <- writeThenReport(order, wg, u, s)
		}()
	}
}
