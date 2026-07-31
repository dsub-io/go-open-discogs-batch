package xmlparser

import (
	"bytes"
	"context"
	"github.com/dsub-io/go-open-discogs-batch/internal/test/resource"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

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
