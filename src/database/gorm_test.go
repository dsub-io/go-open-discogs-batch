package database

import (
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConnect(t *testing.T) {
	t.Run("connect postgres", func(t *testing.T) {
		pg := testutils.GetDatabase(testutils.Postgres)
		dsn := testutils.GetDsn(testutils.Postgres, pg)
		fmt.Println("DSN", dsn)
		err := Connect(dsn)
		assert.NoError(t, err)
		assert.NotNil(t, DB)
		result := DB.Exec("SELECT 1")
		assert.Equal(t, int64(1), result.RowsAffected)
		assert.Nil(t, result.Error)
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
