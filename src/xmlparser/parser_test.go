package xmlparser

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"github.com/dsub-io/go-open-discogs-batch/internal/test/resource"
	"github.com/reactivex/rxgo/v2"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

type tokenResult struct {
	token xml.Token
	err   error
}

type scriptedOrder struct {
	tokens     []tokenResult
	decodeErr  error
	onDecode   func()
	tokenCalls int
	closed     bool
}

func (s *scriptedOrder) Token() (xml.Token, error) {
	s.tokenCalls++
	if len(s.tokens) == 0 {
		return nil, io.EOF
	}
	result := s.tokens[0]
	s.tokens = s.tokens[1:]
	return result.token, result.err
}

func (s *scriptedOrder) DecodeElement(any, *xml.StartElement) error {
	if s.onDecode != nil {
		s.onDecode()
	}
	return s.decodeErr
}

func (s *scriptedOrder) Close() error {
	s.closed = true
	return nil
}

func (s *scriptedOrder) Match(token xml.Token) bool {
	_, ok := token.(xml.StartElement)
	return ok
}

type Data struct {
	ETag        string `xml:"ETag" gorm:"column:etag"`
	GeneratedAt time.Time
	Checksum    string
	TargetType  string `gorm:"column:target_type"`
	Uri         string `xml:"Key"`
}

func Test_parserImpl_Parse(t *testing.T) {
	buf := new(bytes.Buffer)
	buf.Write(resource.Read("testdata/data.xml"))
	parser := NewParser[Data]()
	order := SimpleTokenOrder(io.NopCloser(buf), "Contents")
	count := 0
	for x := range parser.Parse(context.Background(), order).Observe() {
		assert.NotNil(t, x)
		assert.NoError(t, x.E)
		count++
	}
	assert.Equal(t, 777, count)
}

func Test_parserImpl_ParseCtxCancel(t *testing.T) {
	buf := new(bytes.Buffer)
	buf.Write(resource.Read("testdata/data.xml"))
	parser := &parserImpl[Data]{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
	defer cancel()

	count := 0
	for range parser.Parse(ctx, SimpleTokenOrder(io.NopCloser(buf), "Contents")).Observe() {
		count++
	}
	assert.LessOrEqual(t, count, 777)
}

func TestParserHandlesNilAndCancelledContexts(t *testing.T) {
	t.Run("nil context", func(t *testing.T) {
		order := &scriptedOrder{}
		items := NewParser[Data]().Parse(nil, order).Observe()
		_, present := <-items
		assert.False(t, present)
		assert.True(t, order.closed)
	})

	t.Run("already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		order := &scriptedOrder{}
		items := NewParser[Data]().Parse(ctx, order).Observe()
		_, present := <-items
		assert.False(t, present)
		assert.Zero(t, order.tokenCalls)
	})
}

func TestParserEmitsTokenAndDecodeErrors(t *testing.T) {
	t.Run("token error", func(t *testing.T) {
		expected := errors.New("token failure")
		order := &scriptedOrder{tokens: []tokenResult{{err: expected}}}

		item := <-NewParser[Data]().Parse(context.Background(), order).Observe()

		assert.ErrorIs(t, item.E, expected)
	})

	t.Run("decode error", func(t *testing.T) {
		expected := errors.New("decode failure")
		order := &scriptedOrder{
			tokens:    []tokenResult{{token: xml.StartElement{Name: xml.Name{Local: "Contents"}}}},
			decodeErr: expected,
		}

		item := <-NewParser[Data]().Parse(context.Background(), order).Observe()

		assert.ErrorIs(t, item.E, expected)
	})
}

func TestParserStopsWhenContextCancelsDuringDecode(t *testing.T) {
	for _, decodeErr := range []error{nil, errors.New("decode failure")} {
		ctx, cancel := context.WithCancel(context.Background())
		decoded := make(chan struct{})
		releaseDecode := make(chan struct{})
		order := &scriptedOrder{
			tokens:    []tokenResult{{token: xml.StartElement{Name: xml.Name{Local: "Contents"}}}},
			decodeErr: decodeErr,
			onDecode: func() {
				cancel()
				close(decoded)
				<-releaseDecode
			},
		}

		observable := NewParser[Data]().Parse(ctx, order)
		<-decoded
		close(releaseDecode)
		items := observable.Observe()
		_, present := <-items

		assert.False(t, present)
	}
}

func TestEmitStopsWhenCancellationRacesWithDelivery(t *testing.T) {
	originalHook := beforeEmitSelect
	t.Cleanup(func() { beforeEmitSelect = originalHook })
	ctx, cancel := context.WithCancel(context.Background())
	beforeEmitSelect = cancel
	target := make(chan rxgo.Item)

	assert.False(t, emit(ctx, target, rxgo.Of("fixture")))
}

func TestStartTokenMatcherRejectsNilAndNonStartTokens(t *testing.T) {
	matcher := NewStartTokenNameMatcher("Contents")

	assert.False(t, matcher(nil))
	assert.False(t, matcher(xml.CharData("text")))
	assert.False(t, matcher(xml.StartElement{Name: xml.Name{Local: "Other"}}))
	assert.True(t, matcher(xml.StartElement{Name: xml.Name{Local: "Contents"}}))
}
