package cmd

import (
	"fmt"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestValidDsnFormat(t *testing.T) {
	t.Run("no error when valid postgres dsn format", func(t *testing.T) {
		assert.NoError(t, ValidDsnFormat("postgres://user:pass@host:5432/db_name?options"))
		assert.NoError(t, ValidDsnFormat("postgres://user:pass@host:35432/db_name"))
	})
	t.Run("rejects mysql dsn format", func(t *testing.T) {
		assert.Error(t, ValidDsnFormat("mysql://user:pass@host:3306/db_name?options&other=option"))
		assert.Error(t, ValidDsnFormat("mysql://user:pass@host:33060/db_name"))
	})
	t.Run("error when invalid dsn format", func(t *testing.T) {
		assert.Error(t, ValidDsnFormat("mysql://user:pass@host:port/db_name?options"))
	})
	t.Run("error when missing dsn", func(t *testing.T) {
		assert.Error(t, ValidDsnFormat(""))
	})
}

func TestValidTypes(t *testing.T) {
	type args struct {
		types []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "handles missing plural",
			args: args{[]string{"artist", "label", "master", "release"}},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
		{
			name: "no error when empty slice",
			args: args{[]string{}},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
		{
			name: "error contains all unknown types",
			args: args{[]string{"first", "second", "third", "fourth"}},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, fmt.Sprintf("unknown types: [%+v]", "first,second,third,fourth"))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, ValidTypes(tt.args.types), fmt.Sprintf("ValidTypes(%v)", tt.args.types))
		})
	}
}

func TestValidChunkSize(t *testing.T) {
	require.Error(t, ValidChunkSize("-1"))
	require.Error(t, ValidChunkSize("0"))
	require.Error(t, ValidChunkSize("x"))
	require.Error(t, ValidChunkSize(""))
	require.NoError(t, ValidChunkSize("1"))
	require.NoError(t, ValidChunkSize("327564344"))
}

func TestValidYMD(t *testing.T) {
	type args struct {
		y string
		m string
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "invalid year must be reported",
			args: args{y: "999"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "invalid year")
			},
		},
		{
			name: "invalid month must be reported",
			args: args{m: "13"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "invalid month")
			},
		},
		{
			name: "invalid year month prints year only",
			args: args{y: "199x"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "invalid year")
			},
		},
		{
			name: "valid year month does not get error",
			args: args{y: "1993", m: "08"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
		{
			name: "empty args must not get error",
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, ValidYearMonth(tt.args.y, tt.args.m), fmt.Sprintf("ValidYearMonth(%v, %v)", tt.args.y, tt.args.m))
		})
	}
}

func Test_validator_Validate(t *testing.T) {
	invalidDsnConfig := koanf.New(".")
	_ = invalidDsnConfig.Load(rawbytes.Provider([]byte(`dsn: test`)), yaml.Parser())
	invalidYearMonthConfig := invalidDsnConfig.Copy()
	_ = invalidYearMonthConfig.Load(rawbytes.Provider([]byte(`
month: 15
year: 2020`)), yaml.Parser())
	invalidTypesConfig := invalidDsnConfig.Copy()
	_ = invalidTypesConfig.Load(rawbytes.Provider([]byte(`
types:
  - what
  - the
  - hell`)), yaml.Parser())
	validConfig := invalidDsnConfig.Copy()
	_ = validConfig.Load(rawbytes.Provider([]byte(`
dsn: postgres://user:pass@localhost:8802/hello
chunk: 3200
types:
  - artist
year: 2021
month: 12`)), yaml.Parser())

	type args struct {
		koanf *koanf.Koanf
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "must not pass if dsn is missing",
			args: args{
				koanf: koanf.New("."),
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "missing dsn")
			},
		},
		{
			name: "must not pass if invalid dsn provided",
			args: args{invalidDsnConfig},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "invalid")
			},
		},
		{
			name: "must not pass if invalid year or month",
			args: args{invalidYearMonthConfig},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "15")
			},
		},
		{
			name: "must not pass if invalid types has been found",
			args: args{invalidTypesConfig},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorContains(t, err, "what,the,hell")
			},
		},
		{
			name: "must pass if everything looks normal",
			args: args{validConfig},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &validator{}
			tt.wantErr(t, v.Validate(tt.args.koanf), fmt.Sprintf("Validate(%v)", tt.args.koanf))
		})
	}
}
