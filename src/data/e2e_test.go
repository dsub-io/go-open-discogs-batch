//go:build e2e

package data

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

const e2eChecksumManifestURL = "https://data.discogs.com/?download=" +
	"data%2F2026%2Fdiscogs_20260701_CHECKSUM.txt"

func TestDiscogsChecksumManifestIsReachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		e2eChecksumManifestURL,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(
		"User-Agent",
		"GoOpenDiscogsBatch-E2E/1.0 (+https://github.com/dsub-io/go-open-discogs-batch)",
	)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf(
			"Discogs checksum manifest status = %d, want 200: %s",
			response.StatusCode,
			string(body),
		)
	}
	manifest := string(body)
	for _, fileName := range []string{
		"discogs_20260701_artists.xml.gz",
		"discogs_20260701_labels.xml.gz",
		"discogs_20260701_masters.xml.gz",
		"discogs_20260701_releases.xml.gz",
	} {
		if !strings.Contains(manifest, fileName) {
			t.Fatalf("Discogs checksum manifest is missing %s", fileName)
		}
	}
}
