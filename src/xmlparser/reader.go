package xmlparser

import (
	"context"

	"github.com/reactivex/rxgo/v2"
)

// ParseItems reads annotated item of types T with context.Context.
// TokenParseOrder will dictate current io.Reader position and io.Closer for resource
func ParseItems[T any](ctx context.Context, order TokenParseOrder) rxgo.Observable {
	return NewParser[T]().Parse(ctx, order)
}
