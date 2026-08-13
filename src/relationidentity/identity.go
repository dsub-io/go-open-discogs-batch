package relationidentity

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"
	"strings"
)

const (
	canonicalIdentityDomain      = "open-discogs/release-relation-identity/v1"
	compatibilitySlotDomain      = "open-discogs/release-relation-slot/v1"
	fieldNull               byte = 0
	fieldPresent            byte = 1
	domainSeparator         byte = 0
)

// Relation identifies one canonical release relation payload.
type Relation string

const (
	CreditedArtist Relation = "credited_artist"
	Format         Relation = "format"
	Identifier     Relation = "identifier"
	Image          Relation = "image"
	Track          Relation = "track"
	Video          Relation = "video"
	Work           Relation = "work"
)

// Digest is the collision-resistant semantic identity of one relation payload.
type Digest [sha256.Size]byte

// Field preserves the difference between a null field and a present value.
type Field struct {
	value   string
	present bool
}

// NullField returns a canonical null field.
func NullField() Field {
	return Field{}
}

// StringField returns a canonical nullable string field.
func StringField(value *string) Field {
	if value == nil {
		return NullField()
	}
	normalized := strings.TrimFunc(*value, isWhiteSpace)
	if normalized == "" {
		return NullField()
	}
	return Field{value: normalized, present: true}
}

// Int32Field returns a canonical nullable signed decimal field.
func Int32Field(value *int32) Field {
	if value == nil {
		return NullField()
	}
	return Field{value: strconv.FormatInt(int64(*value), 10), present: true}
}

// Sum returns the SHA-256 digest of a framed, relation-specific payload.
func Sum(relation Relation, fields ...Field) Digest {
	hash := sha256.New()
	writeString(hash, canonicalIdentityDomain)
	_, _ = hash.Write([]byte{domainSeparator})
	writeString(hash, string(relation))
	_, _ = hash.Write([]byte{domainSeparator})
	var length [4]byte
	for _, field := range fields {
		if !field.present {
			_, _ = hash.Write([]byte{fieldNull})
			continue
		}
		_, _ = hash.Write([]byte{fieldPresent})
		binary.BigEndian.PutUint32(length[:], uint32(len(field.value)))
		_, _ = hash.Write(length[:])
		writeString(hash, field.value)
	}
	var digest Digest
	copy(digest[:], hash.Sum(nil))
	return digest
}

// Bytes returns an independent byte slice for persistence.
func (digest Digest) Bytes() []byte {
	value := make([]byte, len(digest))
	copy(value, digest[:])
	return value
}

// FromBytes validates and converts a persisted digest.
func FromBytes(value []byte) (Digest, bool) {
	if len(value) != sha256.Size {
		return Digest{}, false
	}
	var digest Digest
	copy(digest[:], value)
	return digest, true
}

// CompatibilitySlot returns a deterministic fallback for a collided legacy hash.
func CompatibilitySlot(relation Relation, digest Digest, attempt uint32) int32 {
	hash := sha256.New()
	writeString(hash, compatibilitySlotDomain)
	_, _ = hash.Write([]byte{domainSeparator})
	writeString(hash, string(relation))
	_, _ = hash.Write([]byte{domainSeparator})
	_, _ = hash.Write(digest[:])
	var encodedAttempt [4]byte
	binary.BigEndian.PutUint32(encodedAttempt[:], attempt)
	_, _ = hash.Write(encodedAttempt[:])
	sum := hash.Sum(nil)
	return int32(binary.BigEndian.Uint32(sum[:4]))
}

type stringWriter interface {
	Write([]byte) (int, error)
}

func writeString(writer stringWriter, value string) {
	_, _ = writer.Write([]byte(value))
}

func isWhiteSpace(value rune) bool {
	switch value {
	case '\u0009', '\u000A', '\u000B', '\u000C', '\u000D',
		'\u0020', '\u0085', '\u00A0', '\u1680',
		'\u2028', '\u2029', '\u202F', '\u205F', '\u3000':
		return true
	default:
		return value >= '\u2000' && value <= '\u200A'
	}
}
