package batch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresSafeBatchSize(t *testing.T) {
	tests := []struct {
		name        string
		requested   int
		columnCount int
		want        int
		wantError   string
	}{
		{name: "keeps safe request", requested: 1_000, columnCount: 15, want: 1_000},
		{name: "caps release rows", requested: 5_000, columnCount: 15, want: 4_369},
		{name: "rejects zero chunk", requested: 0, columnCount: 15, wantError: "chunk size"},
		{name: "rejects empty schema", requested: 1_000, columnCount: 0, wantError: "no columns"},
		{name: "rejects oversized schema", requested: 1, columnCount: 65_536, wantError: "too many columns"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := postgresSafeBatchSize(test.requested, test.columnCount)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
