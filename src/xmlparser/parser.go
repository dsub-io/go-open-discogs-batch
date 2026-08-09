package xmlparser

import (
	"context"
	"encoding/xml"
	"errors"
	"github.com/reactivex/rxgo/v2"
	"io"
)

// Parser abstracts parser object that parses given io.Reader into data.Transform.
// Internal parse mechanism is up to how ParseFunc is implemented.
type Parser[T any] interface {
	Parse(ctx context.Context, order TokenParseOrder) rxgo.Observable
}

// Decoder abstracts *xml.Decoder for xml parse
type Decoder interface {
	Token() (xml.Token, error)
	DecodeElement(v any, start *xml.StartElement) error
}

// TokenParseOrder works as input source, decoder and matcher for given source.
// Match function is the key function to determine target types and value of token.
type TokenParseOrder interface {
	Decoder
	io.Closer
	Match(token xml.Token) bool
}

// SimpleTokenOrder returns TokenParseOrder implementation that matches specific localName of the token from XML stream.
func SimpleTokenOrder(source io.ReadCloser, localName string) TokenParseOrder {
	return &closeDecoderWrapper{xml.NewDecoder(source), source, NewStartTokenNameMatcher(localName)}
}

type closeDecoderWrapper struct {
	decoder Decoder
	closer  io.Closer
	matcher TokenMatcher
}

func (c *closeDecoderWrapper) Token() (xml.Token, error) {
	return c.decoder.Token()
}

func (c *closeDecoderWrapper) DecodeElement(v any, start *xml.StartElement) error {
	return c.decoder.DecodeElement(v, start)
}

func (c *closeDecoderWrapper) Close() error {
	return c.closer.Close()
}

func (c *closeDecoderWrapper) Match(token xml.Token) bool {
	return c.matcher(token)
}

func NewParser[T any]() Parser[T] {
	return &parserImpl[T]{}
}

type parserImpl[T any] struct{}

func (p *parserImpl[T]) Parse(ctx context.Context, order TokenParseOrder) rxgo.Observable {
	if ctx == nil {
		ctx = context.Background()
	}
	c := make(chan rxgo.Item)

	go func(chan rxgo.Item) {
		defer func() {
			close(c)
			_ = order.Close()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if ctx.Err() != nil {
					return
				}
				token, err := order.Token()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						emit(ctx, c, rxgo.Error(err))
					}
					return
				}
				if !order.Match(token) {
					continue
				}
				se := token.(xml.StartElement)
				v := new(T)
				if err := order.DecodeElement(v, &se); err != nil {
					if !emit(ctx, c, rxgo.Error(err)) {
						return
					}
				} else {
					if !emit(ctx, c, rxgo.Of(v)) {
						return
					}
				}
			}
		}
	}(c)
	return rxgo.FromChannel(c)
}

func emit(ctx context.Context, target chan<- rxgo.Item, item rxgo.Item) bool {
	select {
	case <-ctx.Done():
		return false
	case target <- item:
		return true
	}
}
