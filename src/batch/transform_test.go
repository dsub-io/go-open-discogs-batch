package batch

import (
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/reactivex/rxgo/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransform(t *testing.T) {
	t.Run("divide must return valid types", func(t *testing.T) {
		artist := &XmlArtist{}
		for i := range rxgo.Just(artist)().FlatMap(Transform).Observe() {
			assert.IsType(t, &model.Artist{}, i.V)
		}
	})
	t.Run("merge must not return empty or nil", func(t *testing.T) {
		artistRel := &XmlArtistRelation{
			ID:       33,
			NameVars: []string{"test", "hello"},
		}
		count := 0
		for i := range rxgo.Just(artistRel)().FlatMap(Transform).Observe() {
			count++
			assert.IsType(t, &XmlArtistRelation{}, i.V)
		}
		assert.Equal(t, count, 1)
	})

	t.Run("non transformable item must return item as is", func(t *testing.T) {
		count := 0
		for i := range rxgo.Just(10)().FlatMap(Transform).Observe() {
			count++
			assert.Equal(t, 10, i.V)
		}
		assert.Equal(t, 1, count)
	})
}
