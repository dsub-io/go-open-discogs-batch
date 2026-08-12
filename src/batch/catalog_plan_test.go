package batch

import (
	"context"
	"errors"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/src/data"
	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
)

func TestResolveCatalogPlanUsesPinnedCacheWithoutRefresh(t *testing.T) {
	config := catalogPlanConfig(t, "2026-08")
	expected := &data.ImportPlan{}
	resolveCalls := 0
	refreshCalls := 0
	installCatalogPlanSeams(t,
		func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) {
			resolveCalls++
			return expected, nil
		},
		func(context.Context, data.Repository, []string, string) (int, error) {
			refreshCalls++
			return 0, nil
		},
	)

	plan, err := resolveCatalogPlan(context.Background(), config, nil)

	require.NoError(t, err)
	require.Same(t, expected, plan)
	require.Equal(t, 1, resolveCalls)
	require.Zero(t, refreshCalls)
}

func TestResolveCatalogPlanRefreshesMissingPinnedCacheOnce(t *testing.T) {
	config := catalogPlanConfig(t, "2026-08")
	expected := &data.ImportPlan{}
	resolveCalls := 0
	refreshCalls := 0
	installCatalogPlanSeams(t,
		func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) {
			resolveCalls++
			if resolveCalls == 1 {
				return nil, errors.New("cache miss")
			}
			return expected, nil
		},
		func(context.Context, data.Repository, []string, string) (int, error) {
			refreshCalls++
			return 4, nil
		},
	)

	plan, err := resolveCatalogPlan(context.Background(), config, nil)

	require.NoError(t, err)
	require.Same(t, expected, plan)
	require.Equal(t, 2, resolveCalls)
	require.Equal(t, 1, refreshCalls)
}

func TestResolveCatalogPlanReturnsPinnedRefreshAndResolutionErrors(t *testing.T) {
	config := catalogPlanConfig(t, "2026-08")
	cacheErr := errors.New("cache miss")
	refreshErr := errors.New("refresh failed")
	resolveCalls := 0
	installCatalogPlanSeams(t,
		func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) {
			resolveCalls++
			return nil, cacheErr
		},
		func(context.Context, data.Repository, []string, string) (int, error) {
			return 0, refreshErr
		},
	)

	plan, err := resolveCatalogPlan(context.Background(), config, nil)

	require.Nil(t, plan)
	require.ErrorIs(t, err, cacheErr)
	require.ErrorIs(t, err, refreshErr)
	require.Equal(t, 1, resolveCalls)
}

func TestResolveCatalogPlanReturnsPinnedPostRefreshResolutionError(t *testing.T) {
	config := catalogPlanConfig(t, "2026-08")
	cacheErr := errors.New("cache miss")
	resolveErr := errors.New("still missing")
	resolveCalls := 0
	installCatalogPlanSeams(t,
		func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) {
			resolveCalls++
			if resolveCalls == 1 {
				return nil, cacheErr
			}
			return nil, resolveErr
		},
		func(context.Context, data.Repository, []string, string) (int, error) {
			return 4, nil
		},
	)

	plan, err := resolveCatalogPlan(context.Background(), config, nil)

	require.Nil(t, plan)
	require.ErrorIs(t, err, resolveErr)
	require.Equal(t, 2, resolveCalls)
}

func TestResolveCatalogPlanRefreshesLatestBeforeResolution(t *testing.T) {
	config := catalogPlanConfig(t, "")
	expected := &data.ImportPlan{}
	refreshCalls := 0
	resolveCalls := 0
	installCatalogPlanSeams(t,
		func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) {
			resolveCalls++
			return expected, nil
		},
		func(context.Context, data.Repository, []string, string) (int, error) {
			refreshCalls++
			return 4, nil
		},
	)

	plan, err := resolveCatalogPlan(context.Background(), config, nil)

	require.NoError(t, err)
	require.Same(t, expected, plan)
	require.Equal(t, 1, refreshCalls)
	require.Equal(t, 1, resolveCalls)
}

func TestResolveCatalogPlanFallsBackToLatestCache(t *testing.T) {
	config := catalogPlanConfig(t, "")
	expected := &data.ImportPlan{}
	refreshErr := errors.New("refresh failed")
	installCatalogPlanSeams(t,
		func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) {
			return expected, nil
		},
		func(context.Context, data.Repository, []string, string) (int, error) {
			return 0, refreshErr
		},
	)

	plan, err := resolveCatalogPlan(context.Background(), config, nil)

	require.NoError(t, err)
	require.Same(t, expected, plan)
}

func TestResolveCatalogPlanReturnsLatestRefreshAndResolutionErrors(t *testing.T) {
	config := catalogPlanConfig(t, "")
	refreshErr := errors.New("refresh failed")
	resolveErr := errors.New("cache miss")
	installCatalogPlanSeams(t,
		func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error) {
			return nil, resolveErr
		},
		func(context.Context, data.Repository, []string, string) (int, error) {
			return 0, refreshErr
		},
	)

	plan, err := resolveCatalogPlan(context.Background(), config, nil)

	require.Nil(t, plan)
	require.ErrorIs(t, err, refreshErr)
	require.ErrorIs(t, err, resolveErr)
}

func catalogPlanConfig(t *testing.T, dumpMonth string) *koanf.Koanf {
	t.Helper()
	config := koanf.New(".")
	require.NoError(t, config.Set("entities", []string{"artist", "label", "master", "release"}))
	require.NoError(t, config.Set("dump-month", dumpMonth))
	return config
}

func installCatalogPlanSeams(
	t *testing.T,
	resolve func(*koanf.Koanf, data.Repository) (*data.ImportPlan, error),
	refresh func(context.Context, data.Repository, []string, string) (int, error),
) {
	t.Helper()
	originalResolve := resolveImportPlan
	originalRefresh := refreshSelectedData
	t.Cleanup(func() {
		resolveImportPlan = originalResolve
		refreshSelectedData = originalRefresh
	})
	resolveImportPlan = resolve
	refreshSelectedData = refresh
}
