package data

import (
	"errors"
	"github.com/dsub-io/go-open-discogs-batch/internal/testutils"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"strings"
	"testing"
	"time"
)

type DataRepoSuite struct {
	suite.Suite
	DB   *gorm.DB
	repo Repository
}

func (s *DataRepoSuite) SetupSuite() {
	dbInfo := testutils.GetDatabase(s.T(), testutils.Postgres)
	require.NoError(s.T(), database.Connect(testutils.GetDsn(testutils.Postgres, dbInfo)))
	s.DB = database.DB
	require.NoError(s.T(), testutils.ApplySharedSchema(s.DB))
	s.DB.Logger.LogMode(3)
	s.repo = NewDataRepository(s.DB)
}

func TestInit(t *testing.T) {
	suite.Run(t, new(DataRepoSuite))
}

func (s *DataRepoSuite) TestFindByYear() {
	_, err := s.repo.FindByYearMonthType("1900", "03", "artist")
	require.ErrorContains(s.T(), err, "1900")
	require.ErrorContains(s.T(), err, "03")
	require.ErrorContains(s.T(), err, "artist")
}

func (s *DataRepoSuite) TestFindByYearInvalidFormat() {
	_, err := s.repo.FindByYearMonthType("xxxx", "xx", "Xx")
	require.ErrorContains(s.T(), err, "failed to parse")
	require.ErrorContains(s.T(), err, "xxxx.xx")
}

func (s *DataRepoSuite) TestFindLatestByTypeUsesIndependentEntityDate() {
	items := []*Data{
		{
			ETag:        "latest-label-old",
			GeneratedAt: time.Date(2098, 1, 1, 0, 0, 0, 0, time.UTC),
			Checksum:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			TargetType:  "labels",
			Uri:         "data/2098/discogs_20980101_labels.xml.gz",
		},
		{
			ETag:        "latest-label-new",
			GeneratedAt: time.Date(2099, 2, 1, 0, 0, 0, 0, time.UTC),
			Checksum:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			TargetType:  "label",
			Uri:         "data/2099/discogs_20990201_labels.xml.gz",
		},
	}
	_, err := s.repo.BatchInsert(items)
	require.NoError(s.T(), err)
	s.T().Cleanup(func() {
		s.DB.Where("etag IN ?", []string{"latest-label-old", "latest-label-new"}).
			Delete(&opendiscogsmodel.DiscogsDump{})
	})

	latest, err := s.repo.FindLatestByType("labels")
	require.NoError(s.T(), err)
	require.Equal(s.T(), "latest-label-new", latest.ETag)
	require.Equal(s.T(), "label", latest.EntityType)
}

func (s *DataRepoSuite) TestFindLatestByTypeReturnsNotFound() {
	_, err := s.repo.FindLatestByType("never-existing-entity")
	require.ErrorContains(s.T(), err, "latest never-existing-entity data not found")
}

func (s *DataRepoSuite) TestBatchInsertSkipsChecksumOnlyCatalog() {
	count, err := s.repo.BatchInsert([]*Data{{TargetType: "checksum"}})
	require.NoError(s.T(), err)
	require.Zero(s.T(), count)
}

func (s *DataRepoSuite) TestBatchInsertPropagatesDatabaseError() {
	expected := errors.New("fixture")
	errorDB := s.DB.Session(&gorm.Session{})
	errorDB.AddError(expected)
	repository := NewDataRepository(errorDB)

	count, err := repository.BatchInsert([]*Data{{
		GeneratedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		TargetType:  "artist",
		Checksum:    strings.Repeat("a", 64),
		Uri:         "data/2026/discogs_20260701_artists.xml.gz",
	}})
	require.Zero(s.T(), count)
	require.ErrorIs(s.T(), err, expected)
}

func (s *DataRepoSuite) TestBatchInsert() {
	var (
		etag = "test-etag-test-batch-insert"
		gen  = time.Now()
		chk  = "69718470e15145cf586db15389bb2bf81b4cf4ee179aa6c0dd61afaf17d56b3d"
		typ  = "artist"
		uri  = "data/somewhere"
	)
	d := append(make([]*Data, 0), &Data{ETag: etag, GeneratedAt: gen, Checksum: chk, TargetType: typ, Uri: uri})
	count, err := s.repo.BatchInsert(d)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, count)
	var md opendiscogsmodel.DiscogsDump
	s.DB.First(&md)
	require.Equal(s.T(), etag, md.ETag)
	require.Equal(s.T(), gen.Format("20060102"), md.DumpDate.Format("20060102"))
	require.Equal(s.T(), chk, md.ChecksumSHA256)
	require.Equal(s.T(), typ, md.EntityType)
	require.Equal(s.T(), uri, md.URI)

	count, err = s.repo.BatchInsert(d)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, count)
	s.DB.First(&md)
	require.Equal(s.T(), etag, md.ETag)
	require.Equal(s.T(), gen.Format("20060102"), md.DumpDate.Format("20060102"))
	require.Equal(s.T(), chk, md.ChecksumSHA256)
	require.Equal(s.T(), typ, md.EntityType)
	require.Equal(s.T(), uri, md.URI)

	s.DB.Where("etag = ?", etag).Delete(&md)
	assert.ErrorContains(s.T(), s.DB.First(&md).Error, "record not found")
}
