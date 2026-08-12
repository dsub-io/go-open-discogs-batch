package database

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestConnect(t *testing.T) {
	t.Run("connect postgres", func(t *testing.T) {
		pg := testutils.GetDatabase(t, testutils.Postgres)
		dsn := testutils.GetDsn(testutils.Postgres, pg)
		fmt.Println("DSN", dsn)
		err := Connect(dsn)
		assert.NoError(t, err)
		assert.NotNil(t, DB)
		assert.True(t, DB.Config.SkipDefaultTransaction)
		assert.NoError(t, ConfigurePool(DB, 3))
		sqlDB, dbErr := DB.DB()
		assert.NoError(t, dbErr)
		assert.Equal(t, 4, sqlDB.Stats().MaxOpenConnections)
		result := DB.Exec("SELECT 1")
		assert.Equal(t, int64(1), result.RowsAffected)
		assert.Nil(t, result.Error)

		connection, connectErr := GetConnect(dsn)
		assert.NoError(t, connectErr)
		connectionPool, poolErr := connection.DB()
		assert.NoError(t, poolErr)
		assert.NoError(t, connectionPool.Close())

		expected := errors.New("unsupported fixture version")
		unsupportedConnection, unsupportedErr := getConnectInSchema(
			dsn,
			DefaultSchemaName,
			func(*gorm.DB) error { return expected },
		)
		assert.Nil(t, unsupportedConnection)
		assert.ErrorIs(t, unsupportedErr, expected)
	})
	t.Run("reject invalid pool limit", func(t *testing.T) {
		assert.Error(t, ConfigurePool(DB, 0))
	})
	t.Run("must complain", func(t *testing.T) {
		err := Connect("mongo://gorm:LoremIpsum86@localhost:9930?database=dbname")
		assert.ErrorContains(t, err, "unsupported database from dsn: mongo")
	})

	t.Run("must complain", func(t *testing.T) {
		err := Connect("test")
		assert.ErrorContains(t, err, "unsupported dsn. please check again")
	})

	t.Run("reject missing dsn", func(t *testing.T) {
		assert.ErrorContains(t, Connect(""), "missing dsn")
	})

	t.Run("reject invalid schema", func(t *testing.T) {
		_, err := GetConnectInSchema("postgres://user:password@localhost/database", "Invalid")
		assert.ErrorContains(t, err, "database-schema")
	})

	t.Run("reject malformed PostgreSQL dsn", func(t *testing.T) {
		_, err := GetConnectInSchema("postgres://%", DefaultSchemaName)
		assert.ErrorContains(t, err, "parse PostgreSQL DSN")
	})

	t.Run("report unavailable PostgreSQL", func(t *testing.T) {
		_, err := GetConnectInSchema(
			"postgres://invalid:invalid@127.0.0.1:1/invalid?connect_timeout=1",
			DefaultSchemaName,
		)
		assert.Error(t, err)
	})

	t.Run("report unavailable SQL pool", func(t *testing.T) {
		assert.ErrorContains(t, ConfigurePool(&gorm.DB{Config: &gorm.Config{}}, 1), "open SQL connection pool")
	})
}
