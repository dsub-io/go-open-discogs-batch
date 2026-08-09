package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (errorReader) Close() error {
	return nil
}

func TestGetReturnsBodyAndPropagatesContext(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("request"), "catalog")
	client := &clientImpl{wc: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		require.Equal(t, "catalog", request.Context().Value(contextKey("request")))
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader("payload")),
		}, nil
	})}

	item := <-client.Get(ctx, "https://example.test/catalog").Observe()

	require.NoError(t, item.E)
	require.Equal(t, []byte("payload"), item.V)
}

func TestGetRejectsNonSuccessStatus(t *testing.T) {
	client := &clientImpl{wc: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Body:       io.NopCloser(strings.NewReader("unavailable")),
		}, nil
	})}

	item := <-client.Get(context.Background(), "https://example.test/catalog").Observe()

	require.ErrorContains(t, item.E, "503 Service Unavailable")
}

func TestGetPropagatesTransportAndReadErrors(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		client := &clientImpl{wc: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial failed")
		})}

		item := <-client.Get(context.Background(), "https://example.test/catalog").Observe()

		require.ErrorContains(t, item.E, "dial failed")
	})

	t.Run("read", func(t *testing.T) {
		client := &clientImpl{wc: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       errorReader{},
			}, nil
		})}

		item := <-client.Get(context.Background(), "https://example.test/catalog").Observe()

		require.ErrorContains(t, item.E, "read failed")
	})
}
