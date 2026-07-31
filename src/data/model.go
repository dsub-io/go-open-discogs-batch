package data

import (
	"strings"
	"time"

	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
)

type Data struct {
	ETag        string `xml:"ETag" gorm:"column:etag"`
	GeneratedAt time.Time
	Checksum    string
	TargetType  string `gorm:"column:target_type"`
	Uri         string `xml:"Key"`
	SizeBytes   int64  `xml:"Size"`
}

func (d *Data) Dump() *opendiscogsmodel.DiscogsDump {
	return &opendiscogsmodel.DiscogsDump{
		ETag:           strings.Trim(d.ETag, `"`),
		DumpDate:       d.GeneratedAt,
		EntityType:     strings.TrimSuffix(strings.ToLower(d.TargetType), "s"),
		ChecksumSHA256: strings.ToLower(d.Checksum),
		SizeBytes:      d.SizeBytes,
		URI:            d.Uri,
	}
}
