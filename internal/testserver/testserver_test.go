package testserver

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticAndCustomServersRecordRequests(t *testing.T) {
	static := NewServerWithStaticResponse("payload")
	t.Cleanup(static.Close)
	response, err := http.Get(static.URL)
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, "payload", string(body))
	require.Len(t, static.Requests(), 1)
	require.False(t, static.Requests()[0].GetTime().IsZero())

	custom := NewServer(func(requests []*HttpRequest, writer http.ResponseWriter, request *http.Request) {
		require.Len(t, requests, 1)
		writer.WriteHeader(http.StatusCreated)
	})
	t.Cleanup(custom.Close)
	response, err = http.Get(custom.URL)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusCreated, response.StatusCode)
}
