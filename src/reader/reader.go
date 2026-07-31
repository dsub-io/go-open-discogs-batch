package reader

import (
	"context"
	"github.com/dsub-io/go-open-discogs-batch/src/xmlparser"
	"github.com/reactivex/rxgo/v2"
	"io"
)

func NewReader[T any](ctx context.Context, r io.ReadCloser, localName string) rxgo.Observable {
	return xmlparser.ParseItems[T](ctx, xmlparser.SimpleTokenOrder(r, localName))
}
