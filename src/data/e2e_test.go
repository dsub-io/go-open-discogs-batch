//go:build e2e

package data

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

const e2eDumpObjectURL = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com/" +
	"data/2024/discogs_20240701_releases.xml.gz"

func TestDiscogsDumpObjectServesGzipBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		e2eDumpObjectURL,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(
		"User-Agent",
		"GoOpenDiscogsBatch-E2E/1.0 (+https://github.com/dsub-io/go-open-discogs-batch)",
	)
	request.Header.Set("Range", "bytes=0-1")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusPartialContent {
		t.Fatalf(
			"Discogs dump object status = %d, want 206: %s",
			response.StatusCode,
			string(body),
		)
	}
	if got := response.Header.Get("Content-Range"); !strings.HasPrefix(got, "bytes 0-1/") {
		t.Fatalf("Discogs dump Content-Range = %q, want bytes 0-1/...", got)
	}
	if !bytes.Equal(body, []byte{0x1f, 0x8b}) {
		t.Fatalf("Discogs dump prefix = %x, want gzip magic 1f8b", body)
	}
}
