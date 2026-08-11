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
	listing.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	listing.WriteString(`<Contents><Key>data/2026/discogs_20260701_CHECKSUM.txt</Key></Contents>`)
	checksums := new(strings.Builder)
	fixtureByPath := make(map[string]runnerFixture, len(fixtures))
	for _, fixture := range fixtures {
		fmt.Fprintf(listing, `<Contents><Key>data/2026/%s</Key></Contents>`, fixture.filename)
		fmt.Fprintf(checksums, "%s *%s\n", fixture.checksum, fixture.filename)
		fixtureByPath["/data/2026/"+fixture.filename] = fixture
	}
	listing.WriteString(`</ListBucketResult>`)

	server := testserver.NewServer(func(
		requests []*testserver.HttpRequest,
		w http.ResponseWriter,
		r *http.Request,
	) {
		if r.URL.Query().Has("download") {
			_, _ = w.Write([]byte(checksums.String()))
			return
		}
		if fixture, ok := fixtureByPath[r.URL.Path]; ok {
			_, _ = w.Write(fixture.body)
			return
		}
		_, _ = w.Write([]byte(listing.String()))
	})
	defer server.Close()

	originalS3URL, originalDataURL := data.DiscogsS3BaseUrl, data.DiscogsDataBaseURL
	t.Cleanup(func() {
		data.DiscogsS3BaseUrl = originalS3URL
		data.DiscogsDataBaseURL = originalDataURL
	})
	data.DiscogsS3BaseUrl = server.URL + "/"
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
