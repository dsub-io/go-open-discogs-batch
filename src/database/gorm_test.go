package database

import (
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
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
}
