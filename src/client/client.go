package client

import (
	"context"
	"fmt"
	"github.com/reactivex/rxgo/v2"
	"io"
	"net/http"
)

type Client interface {
	Get(ctx context.Context, uri string) rxgo.Observable
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func NewClient() Client {
	return &clientImpl{http.DefaultClient}
}

type clientImpl struct {
	wc httpClient
}

func (c *clientImpl) Get(ctx context.Context, uri string) rxgo.Observable {
	if ctx == nil {
		ctx = context.Background()
	}
	return rxgo.Just(uri)().
		Map(func(_ context.Context, i interface{}) (interface{}, error) {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
			if err != nil {
				return nil, err
			}
			response, err := c.wc.Do(request)
			if err != nil {
				return nil, err
			}
			defer response.Body.Close()
			if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
				return nil, fmt.Errorf("GET %s returned %s", uri, response.Status)
			}
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return nil, fmt.Errorf("read GET %s response: %w", uri, err)
			}
			return body, nil
		})
}
