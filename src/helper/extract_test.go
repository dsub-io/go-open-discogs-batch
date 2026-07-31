package helper

import (
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExtractGormPKColumns(t *testing.T) {
	var m model.Artist
	extractedColumns := ExtractGormPKColumns(m)
	assert.Contains(t, extractedColumns, "id")
	assert.Len(t, extractedColumns, 1)
}
