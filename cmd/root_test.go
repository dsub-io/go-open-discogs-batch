package cmd

import (
	"errors"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_FlagMustOverrideEnv(t *testing.T) {
	t.Setenv("OPEN_DISCOGS_BATCH_DSN", "env_dsn")
	cmd := NewRootCommand()
	cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
	cmd.SetArgs([]string{"--config", "testdata/config.yaml"})
	assert.NoError(t, cmd.Execute())
	s, err := cmd.Flags().GetString("config")
	assert.NoError(t, err)
	assert.Equal(t, "testdata/config.yaml", s)
	cmd.SetArgs([]string{"--config", "testdata/config.yaml", "--dsn", "postgres://user:pass@host:5432/data"})
	assert.NoError(t, cmd.Execute())
	s, err = cmd.Flags().GetString("config")
	assert.NoError(t, err)
	assert.Equal(t, "testdata/config.yaml", s)
}

func Test_getMainFunc(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "getMainFunc returns non nil value",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, getMainFunc())
		})
	}
}

func TestChunkSizePassed(t *testing.T) {
	t.Run("chunk set via environment", func(t *testing.T) {
		cmd := NewRootCommand()
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
		t.Setenv("OPEN_DISCOGS_BATCH_CHUNK", "3500")
		require.NoError(t, cmd.Execute())
		require.Equal(t, 3500, conf.Int("chunk"))
	})
	t.Run("chunk set via flag", func(t *testing.T) {
		cmd := NewRootCommand()
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
		cmd.SetArgs([]string{"--chunk", "5500"})
		require.NoError(t, cmd.Execute())
		require.Equal(t, 5500, conf.Int("chunk"))
	})
}

func Test_getParser(t *testing.T) {
	t.Run(".yaml file gets YamlParser", func(t *testing.T) {
		_, ok := getParser("test.yaml").(*yaml.YAML)
		require.True(t, ok)
		_, ok = getParser("test.yml").(*yaml.YAML)
		require.True(t, ok)
	})
	t.Run(".toml file gets TomlParser", func(t *testing.T) {
		_, ok := getParser("test.toml").(*toml.TOML)
		require.True(t, ok)
		_, ok = getParser("test.tml").(*toml.TOML)
		require.True(t, ok)
	})
	t.Run(".json file gets JsonParser", func(t *testing.T) {
		_, ok := getParser("test.json").(*json.JSON)
		require.True(t, ok)
	})
}

func Test_load(t *testing.T) {
	t.Run("must pluralize types", func(t *testing.T) {
		cmd := NewRootCommand()
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
		conf = koanf.New(".")
		cmd.SetArgs([]string{"-t artist,label,master,release"})
		assert.NoError(t, cmd.Execute())
		for _, typ := range conf.Strings("types") {
			require.Equal(t, "s", typ[len(typ)-1:])
		}
	})
	t.Run("must set default types", func(t *testing.T) {
		cmd := NewRootCommand()
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
		conf = koanf.New(".")
		assert.NoError(t, cmd.Execute())
		types := conf.Strings("types")
		require.Len(t, types, 4)
	})
	t.Run("default type values must be plurals", func(t *testing.T) {
		cmd := NewRootCommand()
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
		conf = koanf.New(".")
		assert.NoError(t, cmd.Execute())
		for _, typ := range conf.Strings("types") {
			require.Equal(t, "s", typ[len(typ)-1:])
		}
	})
	t.Run("types must be set as bool", func(t *testing.T) {
		cmd := NewRootCommand()
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
		conf = koanf.New(".")
		cmd.SetArgs([]string{"-t artists,label"})
		assert.NoError(t, cmd.Execute())
		require.Len(t, conf.Strings("types"), 2)
		require.True(t, conf.Bool("artists"))
		require.True(t, conf.Bool("labels"))
		require.False(t, conf.Bool("masters"))
		require.False(t, conf.Bool("releases"))
	})
}

func TestExecute(t *testing.T) {
	origin := getMainFunc
	defer func() { getMainFunc = origin }()
	getMainFunc = func() func(cmd *cobra.Command, args []string) error {
		return func(cmd *cobra.Command, args []string) error {
			return nil
		}
	}
	assert.NotPanics(t, Execute)
}

func Test_getHomeDir(t *testing.T) {
	assert.NotEmpty(t, getHomeDir(new(homeDirSupplier)))
}

type t1 struct{}

func (t *t1) HomeUserDir() (string, error) {
	return "", errors.New("test error")
}

func Test_getHomeDirPanics(t *testing.T) {
	assert.Panics(t, func() {
		getHomeDir(new(t1))
	})
}
