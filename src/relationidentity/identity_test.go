package relationidentity

import (
	"encoding/hex"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

const (
	fixtureName             = "CD"
	fixtureQuantity         = int32(2)
	fixtureText             = "Compilation"
	fixtureFormatDigest     = "7068967bd82bfe4cae04c757f20a9e8a28bbaf5fa7adc6a71137e3c377ec3d66"
	fixtureTrackSlotAttempt = int32(1335459313)
)

func TestCanonicalDigest(t *testing.T) {
	t.Parallel()

	digest := Sum(
		Format,
		StringField(stringPointer(fixtureName)),
		NullField(),
		Int32Field(int32Pointer(fixtureQuantity)),
		StringField(stringPointer(fixtureText)),
	)
	require.Equal(t, fixtureFormatDigest, hex.EncodeToString(digest[:]))

	joinedLeft := Sum(Track, StringField(stringPointer("AB")), StringField(stringPointer("C")))
	joinedRight := Sum(Track, StringField(stringPointer("A")), StringField(stringPointer("BC")))
	require.NotEqual(t, joinedLeft, joinedRight)
	require.Equal(t, Sum(Track, NullField()), Sum(Track, StringField(stringPointer(""))))
	require.Equal(
		t,
		Sum(Track, StringField(stringPointer("Producer"))),
		Sum(Track, StringField(stringPointer("\u00a0Producer\u3000"))),
	)
	require.NotEqual(
		t,
		Sum(Track, StringField(stringPointer("Pro ducer"))),
		Sum(Track, StringField(stringPointer("Producer"))),
	)
}

func TestCanonicalWhiteSpaceSet(t *testing.T) {
	t.Parallel()

	want := []rune{
		0x0009, 0x000A, 0x000B, 0x000C, 0x000D, 0x0020, 0x0085, 0x00A0, 0x1680,
		0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008,
		0x2009, 0x200A, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000,
	}
	actual := make([]rune, 0, len(want))
	for codePoint := rune(0); codePoint <= unicode.MaxRune; codePoint++ {
		if isWhiteSpace(codePoint) {
			actual = append(actual, codePoint)
		}
	}
	require.Equal(t, want, actual)
}

func TestDigestBytes(t *testing.T) {
	t.Parallel()

	digest := Sum(Work, StringField(stringPointer("Published By")))
	bytes := digest.Bytes()
	require.Len(t, bytes, 32)
	bytes[0]++
	require.NotEqual(t, bytes[0], digest[0])

	parsed, ok := FromBytes(digest[:])
	require.True(t, ok)
	require.Equal(t, digest, parsed)
	_, ok = FromBytes(digest[:31])
	require.False(t, ok)
}

func TestNullableIdentityFieldsPreserveNull(t *testing.T) {
	t.Parallel()

	require.Equal(t, NullField(), StringField(nil))
	require.Equal(t, NullField(), Int32Field(nil))
}

func TestCompatibilitySlot(t *testing.T) {
	t.Parallel()

	digest := Sum(Track, StringField(stringPointer("6")), StringField(stringPointer("Яд")))
	require.Equal(t, fixtureTrackSlotAttempt, CompatibilitySlot(Track, digest, 0))
	require.NotEqual(t, CompatibilitySlot(Track, digest, 0), CompatibilitySlot(Track, digest, 1))
}

func TestCatalogIdentityUsesIndependentDomain(t *testing.T) {
	t.Parallel()

	value := stringPointer("Example")
	digest := CatalogSum(ArtistNameVariation, StringField(value))
	require.NotEqual(t, Sum(Track, StringField(value)), digest)
	require.NotEqual(
		t,
		CatalogCompatibilitySlot(ArtistNameVariation, digest, 0),
		CatalogCompatibilitySlot(ArtistNameVariation, digest, 1),
	)
}

func stringPointer(value string) *string {
	return &value
}

func int32Pointer(value int32) *int32 {
	return &value
}
