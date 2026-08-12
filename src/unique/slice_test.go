package unique

import (
	"errors"
	"testing"

	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_getUniqueSlice(t *testing.T) {
	var (
		artistUrls = []*model.ArtistURL{
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079901, URL: "https://www.instagram.com/legendarydjamar"},
			{ArtistID: 33175, Hash: 1061079902, URL: "https://www.instagram.com/legendarydjamar2"},
		}
		result = Slice(artistUrls)
	)
	assert.Len(t, result, 2)
}

func TestSlicePanicsWhenComparableValueCannotBeHashed(t *testing.T) {
	type unsupported struct {
		Channel chan int
	}

	require.Panics(t, func() { Slice([]unsupported{{Channel: make(chan int)}}) })
}

func TestSlicePreservesDistinctValuesWhenHashesCollide(t *testing.T) {
	items := []string{"first", "second", "first"}
	actual := sliceWithHasher(items, func(string) (uint64, error) { return 1, nil })

	require.Equal(t, []string{"first", "second"}, actual)
}

func TestSlicePanicsWhenHasherFails(t *testing.T) {
	expected := errors.New("fixture")
	require.PanicsWithError(t, expected.Error(), func() {
		sliceWithHasher([]string{"value"}, func(string) (uint64, error) {
			return 0, expected
		})
	})
}
