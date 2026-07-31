package batch

import (
	"context"
	"github.com/dsub-io/go-open-discogs-batch/src/reader"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestReadMaster(t *testing.T) {
	t.Run("must read all 3 items", func(t *testing.T) {
		f, err := os.Open("testdata/master.xml")
		require.NoError(t, err)
		obs := reader.NewReader[XmlMaster](context.Background(), f, "master")
		count := 0
		for m := range obs.Observe() {
			require.NotNil(t, m)
			count++
		}
		require.Equal(t, 3, count)
	})

	t.Run("must read all relations", func(t *testing.T) {
		f, err := os.Open("testdata/master.xml")
		require.NoError(t, err)
		obs := reader.NewReader[XmlMasterRelation](context.Background(), f, "master")
		count := 0
		for m := range obs.Observe() {
			v := m.V.(*XmlMasterRelation)
			for _, vd := range v.Videos {
				require.NotEmpty(t, vd.URL)
				require.NotEmpty(t, vd.Title)
				require.NotEmpty(t, vd.Description)
			}
			require.Greater(t, v.ID, int32(0))
			require.NotEmpty(t, v.Artists)
			require.NotNil(t, m)
			count++
		}
		require.Equal(t, 3, count)
	})
}
