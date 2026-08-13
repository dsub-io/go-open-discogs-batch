package batch

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"github.com/dsub-io/go-open-discogs-batch/src/relationidentity"
	modelschema "github.com/dsub-io/open-discogs-model/schema"
	"github.com/stretchr/testify/require"
)

const (
	releaseIdentityVectorColumns = 11
	releaseIdentityVectorNull    = "null"
	releaseIdentityVectorUnused  = "-"
	releaseIdentityVectorHex     = "hex:"
)

var releaseIdentityRelations = map[string]struct {
	relation   relationidentity.Relation
	fieldCount int
}{
	"credited_artist": {relationidentity.CreditedArtist, 1},
	"format":          {relationidentity.Format, 4},
	"identifier":      {relationidentity.Identifier, 3},
	"image":           {relationidentity.Image, 1},
	"track":           {relationidentity.Track, 3},
	"video":           {relationidentity.Video, 3},
	"work":            {relationidentity.Work, 1},
}

func TestReleaseIdentityMatchesCanonicalModelVectors(t *testing.T) {
	reader := csv.NewReader(bytes.NewReader(modelschema.ReleaseRelationIdentityVectors()))
	reader.Comma = '\t'
	reader.FieldsPerRecord = releaseIdentityVectorColumns
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Greater(t, len(records), 1)
	require.Equal(t, []string{
		"kind", "id", "relation", "field_1", "field_2", "field_3", "field_4",
		"identity_sha256", "attempt", "slot", "expected",
	}, records[0])

	digestRelations := make(map[string]struct{}, len(releaseIdentityRelations))
	for _, record := range records[1:] {
		switch record[0] {
		case "digest":
			digestRelations[record[2]] = struct{}{}
			assertReleaseIdentityDigestVector(t, record)
		case "slot":
			assertReleaseIdentitySlotVector(t, record)
		case "description":
			assertReleaseDescriptionVector(t, record)
		default:
			t.Fatalf("vector %s has unknown kind %q", record[1], record[0])
		}
	}
	require.Len(t, digestRelations, len(releaseIdentityRelations))
	for relation := range releaseIdentityRelations {
		require.Contains(t, digestRelations, relation)
	}
}

func assertReleaseIdentityDigestVector(t *testing.T, record []string) {
	t.Helper()
	contract, exists := releaseIdentityRelations[record[2]]
	require.True(t, exists, "vector %s relation", record[1])
	fields := make([]relationidentity.Field, 0, contract.fieldCount)
	for index, encoded := range record[3:7] {
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
	digest := relationidentity.Sum(contract.relation, fields...)
	require.Equal(t, record[7], hex.EncodeToString(digest[:]), record[1])
}

func assertReleaseIdentitySlotVector(t *testing.T, record []string) {
	t.Helper()
	contract, exists := releaseIdentityRelations[record[2]]
	require.True(t, exists, "vector %s relation", record[1])
	encodedDigest, err := hex.DecodeString(record[7])
	require.NoError(t, err)
	digest, valid := relationidentity.FromBytes(encodedDigest)
	require.True(t, valid)
	attempt, err := strconv.ParseUint(record[8], 10, 32)
	require.NoError(t, err)
	want, err := strconv.ParseInt(record[9], 10, 32)
	require.NoError(t, err)
	require.Equal(
		t,
		int32(want),
		relationidentity.CompatibilitySlot(contract.relation, digest, uint32(attempt)),
		record[1],
	)
}

func assertReleaseDescriptionVector(t *testing.T, record []string) {
	t.Helper()
	values := make([]string, 0, 4)
	for _, encoded := range record[3:7] {
		value := decodeReleaseIdentityVector(t, record[1], encoded)
		values = append(values, string(value))
	}
	actual := reducedDescription(values)
	wantBytes := decodeReleaseIdentityVector(t, record[1], record[10])
	if wantBytes == nil {
		require.Nil(t, actual, record[1])
		return
	}
	require.NotNil(t, actual, record[1])
	require.Equal(t, string(wantBytes), *actual, record[1])
}

func decodeReleaseIdentityVector(t *testing.T, id, encoded string) []byte {
	t.Helper()
	if encoded == releaseIdentityVectorNull {
		return nil
	}
	require.True(t, strings.HasPrefix(encoded, releaseIdentityVectorHex), id)
	value, err := hex.DecodeString(strings.TrimPrefix(encoded, releaseIdentityVectorHex))
	require.NoError(t, err, id)
	return value
}
