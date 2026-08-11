package batch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/internal/testserver"
	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/data"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
)

type runnerFixture struct {
	entity   string
	filename string
	body     []byte
	checksum string
}

func TestRunnerEndToEndAndSuccessfulSkip(t *testing.T) {
	const customSchema = "open_discogs"
	fixtures := make([]runnerFixture, 0, 4)
	for _, entity := range []string{"artist", "label", "master", "release"} {
		filename := fmt.Sprintf("discogs_20260701_%ss.xml.gz", entity)
		body, err := os.ReadFile(filepath.Join("testdata", entity+".xml.gz"))
		require.NoError(t, err)
		fixtures = append(fixtures, runnerFixture{
			entity:   entity,
			filename: filename,
			body:     body,
			checksum: fmt.Sprintf("%x", sha256.Sum256(body)),
		})
	}

	listing := new(strings.Builder)
	listing.WriteString(`<a href="?download=data%2F2026%2Fdiscogs_20260701_CHECKSUM.txt">checksum</a>`)
	checksums := new(strings.Builder)
	fixtureByURI := make(map[string]runnerFixture, len(fixtures))
	for _, fixture := range fixtures {
		fmt.Fprintf(listing, `<a href="?download=data%%2F2026%%2F%s">%s</a>`, fixture.filename, fixture.filename)
		fmt.Fprintf(checksums, "%s *%s\n", fixture.checksum, fixture.filename)
		fixtureByURI["data/2026/"+fixture.filename] = fixture
	}

	server := testserver.NewServer(func(
		requests []*testserver.HttpRequest,
		w http.ResponseWriter,
		r *http.Request,
	) {
		download := r.URL.Query().Get("download")
		if strings.HasSuffix(download, "CHECKSUM.txt") {
			_, _ = w.Write([]byte(checksums.String()))
			return
		}
		if fixture, ok := fixtureByURI[download]; ok {
			_, _ = w.Write(fixture.body)
			return
		}
		_, _ = w.Write([]byte(listing.String()))
	})
	defer server.Close()

	originalDataURL := data.DiscogsDataBaseURL
	t.Cleanup(func() { data.DiscogsDataBaseURL = originalDataURL })
	data.DiscogsDataBaseURL = server.URL + "/"

	postgres := testutils.GetDatabase(t, testutils.Postgres)
	config := koanf.New(".")
	for key, value := range map[string]interface{}{
		"database-url":    testutils.GetDsn(testutils.Postgres, postgres),
		"database-schema": customSchema,
		"entities":        []string{"artist", "label", "master", "release"},
		"dump-month":      "2026-07",
		"data-dir":        t.TempDir(),
		"chunk-size":      5,
		"max-workers":     2,
		"cleanup":         true,
		"force":           false,
		"allow-downgrade": false,
		"artists":         true,
		"labels":          true,
		"masters":         true,
		"releases":        true,
	} {
		require.NoError(t, config.Set(key, value))
	}

	runner := &Runner{Version: "test"}
	require.NoError(t, runner.Run(context.Background(), config))
	var artistCount int64
	require.NoError(t, database.DB.Raw(
		`select count(*) from "open_discogs"."artist"`,
	).Scan(&artistCount).Error)
	require.Positive(t, artistCount)
	var publicArtistTable *string
	require.NoError(t, database.DB.Raw(
		`select to_regclass('public.artist')::text`,
	).Scan(&publicArtistTable).Error)
	require.Nil(t, publicArtistTable)
	for _, fixture := range fixtures {
		require.NoFileExists(t, filepath.Join(config.String("data-dir"), fixture.filename))
	}

	require.NoError(t, runner.Run(context.Background(), config))
	require.NoError(t, config.Set("cleanup", false))
	require.NoError(t, runner.Run(context.Background(), config))
}
