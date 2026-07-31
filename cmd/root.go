package cmd

import (
	"context"
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/src/batch"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"os"
	"strings"
	"time"
)

const Prefix = "OPEN_DISCOGS_BATCH_"

const versionPrintPrefix = "go-open-discogs-batch"

var version string
var conf = koanf.New(".")
var sep = string(os.PathSeparator)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := NewRootCommand().Execute()
	if err != nil {
		fmt.Printf("critical error: %+v\n", err.Error())
	}
}

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "go-open-discogs-batch",
		Short: "Import OpenDiscogs data dumps into PostgreSQL",
		Long: `go-open-discogs-batch imports selected OpenDiscogs data dumps
into the canonical PostgreSQL schema published by open-discogs-model.`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return load(cmd.Flags(), conf)
		},
		RunE: getMainFunc(),
	}
	f := rootCmd.Flags()
	y, m := time.Now().Format("2006"), time.Now().Format("01")
	home := getHomeDir(new(homeDirSupplier))
	home += sep + "go-open-discogs-batch"
	f.BoolP("new", "n", false, "generates tables before batch")
	f.StringP("config", "c", home+sep+"config.yaml", "config file path")
	f.StringP("data", "d", home, "data file dir")
	f.StringSliceP("types", "t", []string{"artists", "labels", "masters", "releases"}, "target types")
	f.StringP("year", "y", y, "target year")
	f.StringP("month", "m", m, "target month")
	f.BoolP("version", "v", false, "prints version")
	f.IntP("chunk", "b", 5000, "chunk size")
	f.BoolP("update", "u", false, "update data repo")
	f.BoolP("purge", "p", false, "purge files after success")
	f.BoolP("force", "f", false, "rerun a manifest that already completed successfully")
	f.Bool("allow-downgrade", false, "allow an older entity dump and record the override")
	f.StringP("dsn", "s", "", "PostgreSQL data source name")
	return rootCmd
}

var getMainFunc = func() func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if ok, _ := cmd.Flags().GetBool("version"); ok {
			fmt.Println(versionPrintPrefix, version)
			return nil
		}
		if err := new(validator).Validate(conf); err != nil {
			return err
		}
		return (&batch.Runner{Version: version}).Run(context.Background(), conf)
	}
}

func getFilename(path string) string {
	return getLastPart(path, string(os.PathSeparator))
}

func getFileExtension(path string) string {
	return getLastPart(getFilename(path), ".")
}

func getLastPart(s, delim string) string {
	if len(s) == 0 {
		return ""
	}
	parts := strings.Split(s, delim)
	return parts[len(parts)-1]
}

func getTrimEnvPrefixFunc(prefix string) func(key string) string {
	return func(key string) string {
		return strings.Replace(strings.ToLower(strings.TrimPrefix(key, prefix)), "_", ".", -1)
	}
}

func getParser(filepath string) koanf.Parser {
	ex := getFileExtension(filepath)
	switch ex {
	case "yaml", "yml":
		return yaml.Parser()
	case "toml", "tml":
		return toml.Parser()
	}
	return json.Parser()
}

func load(flags *pflag.FlagSet, k *koanf.Koanf) error {
	if ok, _ := flags.GetBool("version"); ok {
		return nil
	}
	loadConfigFile(k, getConfigFilePath(flags))
	if err := loadEnvironment(k); err != nil {
		return err
	}
	if err := loadFlags(flags, k); err != nil {
		return err
	}
	fixTypesAsPlural(k)
	return setTypesIntoBooleans(k)
}

func setTypesIntoBooleans(k *koanf.Koanf) error {
	yml := ""
	for _, typ := range k.Strings("types") {
		yml += fmt.Sprintf("%+v: true\n", typ)
	}
	return k.Load(rawbytes.Provider([]byte(yml)), yaml.Parser())
}

func getConfigFilePath(flags *pflag.FlagSet) string {
	path, _ := flags.GetString("config")
	return path
}

func loadConfigFile(k *koanf.Koanf, path string) {
	if err := k.Load(file.Provider(path), getParser(path)); err == nil {
		fmt.Printf("located config [%+v]: loaded\n", path)
	} else {
		fmt.Printf("failed to locate config [%+v]: skipping\n", path)
	}
}

func loadFlags(flags *pflag.FlagSet, k *koanf.Koanf) error {
	return k.Load(posflag.Provider(flags, ".", k), nil)
}

func loadEnvironment(k *koanf.Koanf) error {
	if err := k.Load(env.Provider(Prefix, ".", getTrimEnvPrefixFunc(Prefix)), nil); err != nil {
		return err
	}
	return nil
}

func getHomeDir(supplier HomeDirSupplier) string {
	if home, err := supplier.HomeUserDir(); err != nil {
		panic("failed to determine home directory")
	} else {
		return home
	}
}

type HomeDirSupplier interface {
	HomeUserDir() (string, error)
}

type homeDirSupplier struct{}

func (h *homeDirSupplier) HomeUserDir() (string, error) {
	return os.UserHomeDir()
}
