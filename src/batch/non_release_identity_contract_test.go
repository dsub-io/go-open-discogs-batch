package batch

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/src/relationidentity"
	modelschema "github.com/dsub-io/open-discogs-model/schema"
	"github.com/stretchr/testify/require"
)

const nonReleaseIdentityVectorColumns = 10

var nonReleaseIdentityRelations = map[string]struct {
	relation   relationidentity.CatalogRelation
	fieldCount int
}{
	"artist_name_variation": {relationidentity.ArtistNameVariation, 1},
	"artist_url":            {relationidentity.ArtistURL, 1},
	"label_url":             {relationidentity.LabelURL, 1},
	"master_video":          {relationidentity.MasterVideo, 3},
}

func TestNonReleaseIdentityMatchesCanonicalModelVectors(t *testing.T) {
	reader := csv.NewReader(bytes.NewReader(modelschema.NonReleaseRelationIdentityVectors()))
	reader.Comma = '\t'
	reader.FieldsPerRecord = nonReleaseIdentityVectorColumns
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Greater(t, len(records), 1)
	require.Equal(t, []string{
		"kind", "id", "relation", "field_1", "field_2", "field_3",
		"identity_sha256", "attempt", "slot", "legacy_java_hash",
	}, records[0])

	digestRelations := make(map[string]struct{}, len(nonReleaseIdentityRelations))
	for _, record := range records[1:] {
		switch record[0] {
		case "digest":
			digestRelations[record[2]] = struct{}{}
			assertNonReleaseIdentityDigestVector(t, record)
		case "slot":
			assertNonReleaseIdentitySlotVector(t, record)
		default:
			t.Fatalf("vector %s has unknown kind %q", record[1], record[0])
		}
	}
	require.Len(t, digestRelations, len(nonReleaseIdentityRelations))
	for relation := range nonReleaseIdentityRelations {
		require.Contains(t, digestRelations, relation)
	}
}

func assertNonReleaseIdentityDigestVector(t *testing.T, record []string) {
	t.Helper()
	contract, exists := nonReleaseIdentityRelations[record[2]]
	require.True(t, exists, "vector %s relation", record[1])
	fields := make([]relationidentity.Field, 0, contract.fieldCount)
	for index, encoded := range record[3:6] {
		if index >= contract.fieldCount {
			require.Equal(t, releaseIdentityVectorUnused, encoded, record[1])
			continue
		}
		value := decodeReleaseIdentityVector(t, record[1], encoded)
		if value == nil {
			fields = append(fields, relationidentity.NullField())
			continue
		}
		text := string(value)
		fields = append(fields, relationidentity.StringField(&text))
	}
	digest := relationidentity.CatalogSum(contract.relation, fields...)
	require.Equal(t, record[6], hex.EncodeToString(digest[:]), record[1])
}

func assertNonReleaseIdentitySlotVector(t *testing.T, record []string) {
	t.Helper()
	contract, exists := nonReleaseIdentityRelations[record[2]]
	require.True(t, exists, "vector %s relation", record[1])
	encodedDigest, err := hex.DecodeString(record[6])
	require.NoError(t, err)
	digest, valid := relationidentity.FromBytes(encodedDigest)
	require.True(t, valid)
	attempt, err := strconv.ParseUint(record[7], 10, 32)
	require.NoError(t, err)
	want, err := strconv.ParseInt(record[8], 10, 32)
	require.NoError(t, err)
	require.Equal(
		t,
		int32(want),
		relationidentity.CatalogCompatibilitySlot(contract.relation, digest, uint32(attempt)),
		record[1],
	)
}
