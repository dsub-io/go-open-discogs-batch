package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"testing"
)

func TestBatch(t *testing.T) {
	pg := testutils.GetDatabase(t, testutils.Postgres)
	dsn := testutils.GetDsn(testutils.Postgres, pg)
	db, err := database.GetConnect(dsn)
	require.NoError(t, err)
	require.NoError(t, RunDDL(db))

	var (
		ctx        = context.Background()
		chunk      = 5
		maxWorkers = 2
	)
	order := NewOrder(ctx, chunk, maxWorkers, "testdata/artist.xml.gz", db)

	res := newBatch().UpdateArtist(order)()
	require.NoError(t, res.Err())
	require.NotZero(t, res.Count())

	order = NewOrder(ctx, chunk, maxWorkers, "testdata/label.xml.gz", db)

	res = newBatch().UpdateLabel(order)()
	require.NoError(t, res.Err())
	require.NotZero(t, res.Count())

	order = NewOrder(ctx, chunk, maxWorkers, "testdata/master.xml.gz", db)
	res = newBatch().UpdateMaster(order)()
	require.NoError(t, res.Err())
	require.NotZero(t, res.Count())

	var masters []*model.Master
	db.Session(&gorm.Session{}).Find(&masters)
	require.NotEmpty(t, masters)

	for _, master := range masters {
		var count int64
		db.Session(&gorm.Session{}).Model(&model.MasterStyle{}).Where("master_id = ?", master.ID).Count(&count)
		require.NotZero(t, count)
		db.Session(&gorm.Session{}).Model(&model.MasterGenre{}).Where("master_id = ?", master.ID).Count(&count)
		require.NotZero(t, count)
	}

	order = NewOrder(ctx, chunk, maxWorkers, "testdata/release.xml.gz", db)
	res = newBatch().UpdateRelease(order)()
	require.NoError(t, res.Err())

	var count int64
	db.Session(&gorm.Session{}).Model(&model.ReleaseItem{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemWork{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemIdentifier{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemFormat{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemTrack{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemCreditedArtist{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemVideo{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.LabelReleaseItem{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemArtist{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemGenre{}).Count(&count)
	require.NotZero(t, count)
	db.Session(&gorm.Session{}).Model(&model.ReleaseItemStyle{}).Count(&count)
	require.NotZero(t, count)

	before := snapshotBusinessTables(t, db)
	for _, fixture := range []struct {
		path string
		step func(Order) Step
	}{
		{"testdata/artist.xml.gz", newBatch().UpdateArtist},
		{"testdata/label.xml.gz", newBatch().UpdateLabel},
		{"testdata/master.xml.gz", newBatch().UpdateMaster},
		{"testdata/release.xml.gz", newBatch().UpdateRelease},
	} {
		repeated := fixture.step(NewOrder(ctx, chunk, maxWorkers, fixture.path, db))()
		require.NoError(t, repeated.Err())
	}
	after := snapshotBusinessTables(t, db)
	require.Equal(t, before, after)

	normalized := normalizedBusinessState(t, db)
	goldenPath := filepath.Join("testdata", "cross-language-state.json")
	if os.Getenv("UPDATE_CROSS_LANGUAGE_GOLDEN") == "1" {
		encoded, err := json.MarshalIndent(normalized, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(goldenPath, append(encoded, '\n'), 0o644))
	}
	expected, err := os.ReadFile(goldenPath)
	require.NoError(t, err)
	actual, err := json.Marshal(normalized)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(actual))
}

func snapshotBusinessTables(t *testing.T, db *gorm.DB) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	for _, table := range model.TableNames {
		if len(table) >= len("discogs_") && table[:len("discogs_")] == "discogs_" {
			continue
		}
		var rows string
		query := fmt.Sprintf(
			`select coalesce(jsonb_agg(to_jsonb(row_data) order by to_jsonb(row_data)::text), '[]'::jsonb)::text
			   from public.%s row_data`,
			table,
		)
		require.NoError(t, db.Raw(query).Scan(&rows).Error)
		snapshot[table] = rows
	}
	return snapshot
}

func normalizedBusinessState(
	t *testing.T,
	db *gorm.DB,
) map[string]json.RawMessage {
	t.Helper()
	state := make(map[string]json.RawMessage)
	coreTables := map[string]bool{
		"artist":       true,
		"label":        true,
		"master":       true,
		"release_item": true,
	}
	for _, table := range model.TableNames {
		if len(table) >= len("discogs_") && table[:len("discogs_")] == "discogs_" {
			continue
		}
		projection := "to_jsonb(row_data) - 'created_at' - 'last_modified_at'"
		if !coreTables[table] && table != "genre" && table != "style" {
			projection += " - 'id'"
		}
		var rows string
		query := fmt.Sprintf(
			`select coalesce(jsonb_agg(projected order by projected::text), '[]'::jsonb)::text
			   from (
			       select %s as projected
			         from public.%s row_data
			   ) normalized`,
			projection,
			table,
		)
		require.NoError(t, db.Raw(query).Scan(&rows).Error)
		state[table] = json.RawMessage(rows)
	}
	return state
}

func Test_batch_UpdateLabel(t *testing.T) {
	type fields struct {
		db *gorm.DB
	}
	type args struct {
		order Order
	}
	var tests []struct {
		name   string
		fields fields
		args   args
		want   Step
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &batch{
				db: tt.fields.db,
			}
			assert.Equalf(t, tt.want, b.UpdateLabel(tt.args.order), "UpdateLabel(%v)", tt.args.order)
		})
	}
}

func Test_batch_UpdateMaster(t *testing.T) {
	type fields struct {
		db *gorm.DB
	}
	type args struct {
		order Order
	}
	var tests []struct {
		name   string
		fields fields
		args   args
		want   Step
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &batch{
				db: tt.fields.db,
			}
			assert.Equalf(t, tt.want, b.UpdateMaster(tt.args.order), "UpdateMaster(%v)", tt.args.order)
		})
	}
}

func Test_batch_UpdateRelease(t *testing.T) {
	type fields struct {
		db *gorm.DB
	}
	type args struct {
		order Order
	}
	var tests []struct {
		name   string
		fields fields
		args   args
		want   Step
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &batch{
				db: tt.fields.db,
			}
			assert.Equalf(t, tt.want, b.UpdateRelease(tt.args.order), "UpdateRelease(%v)", tt.args.order)
		})
	}
}
