package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/knadh/koanf"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func executeWithoutBatch(t *testing.T, args ...string) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
	cmd.SetArgs(args)
	require.NoError(t, cmd.Execute())
}

func TestPublicDefaults(t *testing.T) {
	executeWithoutBatch(t)

	require.Equal(t, []string{"artist", "label", "master", "release"}, conf.Strings("entities"))
	require.Equal(t, 5000, conf.Int("chunk-size"))
	require.Equal(t, runtime.GOMAXPROCS(0), conf.Int("max-workers"))
	require.Equal(t, filepath.Join(getHomeDir(new(homeDirSupplier)), ".cache", "open-discogs-batch"), conf.String("data-dir"))
	require.False(t, conf.Bool("cleanup"))
}

func TestEnvironmentVariablesAndCommandLinePrecedence(t *testing.T) {
	t.Setenv("OPEN_DISCOGS_BATCH_DATABASE_URL", "postgresql://env:pass@db:5432/discogs")
	t.Setenv("OPEN_DISCOGS_BATCH_ENTITIES", "artist, release")
	t.Setenv("OPEN_DISCOGS_BATCH_DUMP_MONTH", "2026-07")
	t.Setenv("OPEN_DISCOGS_BATCH_DATA_DIR", "/env-data")
	t.Setenv("OPEN_DISCOGS_BATCH_CHUNK_SIZE", "3500")
	t.Setenv("OPEN_DISCOGS_BATCH_MAX_WORKERS", "7")
	t.Setenv("OPEN_DISCOGS_BATCH_CLEANUP", "true")
	t.Setenv("OPEN_DISCOGS_BATCH_FORCE", "true")
	t.Setenv("OPEN_DISCOGS_BATCH_ALLOW_DOWNGRADE", "true")

	executeWithoutBatch(
		t,
		"--database-url", "postgresql://cli:pass@db:5432/discogs",
		"--entities", "label,master",
		"--chunk-size", "5500",
		"--max-workers", "3",
		"--data-dir", "/cli-data",
	)

	require.Equal(t, "postgresql://cli:pass@db:5432/discogs", conf.String("database-url"))
	require.Equal(t, []string{"label", "master"}, conf.Strings("entities"))
	require.Equal(t, "2026-07", conf.String("dump-month"))
	require.Equal(t, "/cli-data", conf.String("data-dir"))
	require.Equal(t, 5500, conf.Int("chunk-size"))
	require.Equal(t, 3, conf.Int("max-workers"))
	require.True(t, conf.Bool("cleanup"))
	require.True(t, conf.Bool("force"))
	require.True(t, conf.Bool("allow-downgrade"))
	require.False(t, conf.Bool("artists"))
	require.True(t, conf.Bool("labels"))
	require.True(t, conf.Bool("masters"))
	require.False(t, conf.Bool("releases"))
}

func TestLegacyFlagsAreRejected(t *testing.T) {
	for _, legacy := range []string{"--config", "--dsn", "--types", "--year", "--month", "--purge", "--new", "--update"} {
		cmd := NewRootCommand()
		cmd.SetArgs([]string{legacy})
		require.Error(t, cmd.Execute(), legacy)
	}
}

func TestVersionDoesNotRequireDatabaseURL(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"--version"})
	require.NoError(t, cmd.Execute())
}

func TestGetHomeDir(t *testing.T) {
	require.NotEmpty(t, getHomeDir(new(homeDirSupplier)))
}

type failingHomeDirSupplier struct{}

func (failingHomeDirSupplier) HomeUserDir() (string, error) {
	return "", errors.New("test error")
}

func TestGetHomeDirPanics(t *testing.T) {
	require.Panics(t, func() { getHomeDir(failingHomeDirSupplier{}) })
}

func TestExecuteUsesProcessArguments(t *testing.T) {
	originalArgs := os.Args
	os.Args = []string{"go-open-discogs-batch", "--version"}
	t.Cleanup(func() { os.Args = originalArgs })

	require.NoError(t, Execute())
}

func TestMainFunctionValidatesAndDelegates(t *testing.T) {
	originalRunBatch := runBatch
	t.Cleanup(func() { runBatch = originalRunBatch })
	invalidConfig := koanf.New(".")
	require.NoError(t, invalidConfig.Set("database-url", "://invalid"))
	require.Error(t, originalRunBatch(context.Background(), invalidConfig, "test"))

	called := false
	runBatch = func(ctx context.Context, config *koanf.Koanf, releaseVersion string) error {
		called = true
		require.NotNil(t, ctx)
		return nil
	}

	command := NewRootCommand()
	command.SetArgs([]string{
		"--database-url", "postgresql://user:pass@db:5432/discogs",
		"--entities", "artist",
	})
	require.NoError(t, command.Execute())
	require.True(t, called)

	command = NewRootCommand()
	command.SetArgs([]string{"--entities", "artist"})
	require.ErrorContains(t, command.Execute(), "database-url")
}

func TestPrintableVersion(t *testing.T) {
	originalVersion := version
	t.Cleanup(func() { version = originalVersion })
	version = "  "
	require.Equal(t, "development", printableVersion())
	version = "1.2.3"
	require.Equal(t, "1.2.3", printableVersion())
}

func TestLoadPropagatesEachStageFailure(t *testing.T) {
	originalEnvironment := loadEnvironmentConfig
	originalFlags := loadFlagConfig
	originalNormalize := normalizeEntityConfig
	t.Cleanup(func() {
		loadEnvironmentConfig = originalEnvironment
		loadFlagConfig = originalFlags
		normalizeEntityConfig = originalNormalize
	})
	flags := pflag.NewFlagSet("fixture", pflag.ContinueOnError)
	flags.Bool("version", false, "")
	config := koanf.New(".")
	expected := errors.New("fixture")

	loadEnvironmentConfig = func(*koanf.Koanf) error { return expected }
	require.ErrorIs(t, load(flags, config), expected)

	loadEnvironmentConfig = originalEnvironment
	loadFlagConfig = func(*pflag.FlagSet, *koanf.Koanf) error { return expected }
	require.ErrorIs(t, load(flags, config), expected)

	loadFlagConfig = originalFlags
	normalizeEntityConfig = func(*koanf.Koanf) error { return expected }
	require.ErrorIs(t, load(flags, config), expected)
}

func TestNormalizeEntitiesPropagatesSetFailures(t *testing.T) {
	originalList := setEntityListConfig
	originalSelection := setEntitySelectionConfig
	t.Cleanup(func() {
		setEntityListConfig = originalList
		setEntitySelectionConfig = originalSelection
	})
	config := koanf.New(".")
	require.NoError(t, config.Set("entities", []string{"artist"}))
	expected := errors.New("set failure")

	setEntityListConfig = func(*koanf.Koanf, []string) error { return expected }
	require.ErrorIs(t, normalizeEntities(config), expected)

	setEntityListConfig = originalList
	setEntitySelectionConfig = func(*koanf.Koanf, string, bool) error { return expected }
	require.ErrorIs(t, normalizeEntities(config), expected)
}

func TestLoadEnvironmentHandlesUnknownAndBooleanSpellings(t *testing.T) {
	t.Setenv(Prefix+"UNKNOWN", "ignored")
	t.Setenv(Prefix+"CLEANUP", "off")
	t.Setenv(Prefix+"FORCE", "1")
	t.Setenv(Prefix+"ALLOW_DOWNGRADE", "invalid")
	config := koanf.New(".")

	require.NoError(t, loadEnvironment(config))
	require.False(t, config.Exists("unknown"))
	require.False(t, config.Bool("cleanup"))
	require.True(t, config.Bool("force"))
	require.Equal(t, "invalid", config.String("allow-downgrade"))
}

func TestNormalizeEntitiesDropsBlankAndDuplicateValues(t *testing.T) {
	config := koanf.New(".")
	require.NoError(t, config.Set("entities", []string{"", "Artists", "artist", " RELEASES "}))

	require.NoError(t, normalizeEntities(config))
	require.Equal(t, []string{"artist", "release"}, config.Strings("entities"))
	require.True(t, config.Bool("artists"))
	require.True(t, config.Bool("releases"))
}
