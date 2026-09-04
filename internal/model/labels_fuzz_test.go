package model

import (
	"encoding/binary"
	"testing"
)

// FuzzLabelSetCanonicalIsDecodable proves injectivity of the canonical encoding the
// only way that generalizes: by decoding it.
//
// An encoding that can be unambiguously decoded cannot map two different label sets
// to the same bytes, and therefore cannot merge two streams into one stream_id.
// Asserting "these two hand-picked label sets do not collide" only covers the cases
// someone thought of; this covers the ones they did not, including arbitrary bytes
// inside names and values.
func FuzzLabelSetCanonicalIsDecodable(f *testing.F) {
	f.Add("api", "node-1", "prod", "region", "eu-west-1")
	f.Add("", "", "", "", "")
	f.Add("a|b", "c", "d", "e", "f")
	f.Add("a\x00b", "c", "d", "k\x00", "\x00v")
	f.Add("s", "h", "e", "k", "")

	f.Fuzz(func(t *testing.T, service, host, env, key, value string) {
		ls := LabelSet{Service: service, Host: host, Env: env}
		if key != "" {
			ls.Extra = map[string]string{key: value}
		}

		encoded := ls.AppendCanonical(nil)
		decoded, rest, err := decodeCanonical(encoded)
		if err != nil {
			t.Fatalf("encoding of %s is not decodable: %v", ls, err)
		}
		if len(rest) != 0 {
			t.Fatalf("encoding of %s left %d trailing bytes", ls, len(rest))
		}

		if decoded.Service != service || decoded.Host != host || decoded.Env != env {
			t.Fatalf("promoted labels did not round trip: %s", decoded)
		}
		if len(decoded.Extra) != len(ls.Extra) {
			t.Fatalf("extra label count changed: %d != %d", len(decoded.Extra), len(ls.Extra))
		}
		if key != "" && decoded.Extra[key] != value {
			t.Fatalf("extra label %q = %q, want %q", key, decoded.Extra[key], value)
		}
	})
}

// decodeCanonical is the inverse of LabelSet.AppendCanonical. It exists only in
// tests: production code never needs to read the encoding back, it only hashes it.
func decodeCanonical(b []byte) (LabelSet, []byte, error) {
	var ls LabelSet
	var err error

	if ls.Service, b, err = decodeField(b); err != nil {
		return ls, b, err
	}
	if ls.Host, b, err = decodeField(b); err != nil {
		return ls, b, err
	}
	if ls.Env, b, err = decodeField(b); err != nil {
		return ls, b, err
	}

	count, n := binary.Uvarint(b)
	if n <= 0 {
		return ls, b, errShortEncoding
	}
	b = b[n:]

	if count > 0 {
		ls.Extra = make(map[string]string, count)
	}
	for i := uint64(0); i < count; i++ {
		var name, value string
		if name, b, err = decodeField(b); err != nil {
			return ls, b, err
		}
		if value, b, err = decodeField(b); err != nil {
			return ls, b, err
		}
		ls.Extra[name] = value
	}
	return ls, b, nil
}

func decodeField(b []byte) (string, []byte, error) {
	length, n := binary.Uvarint(b)
	if n <= 0 {
		return "", b, errShortEncoding
	}
	b = b[n:]
	if uint64(len(b)) < length {
		return "", b, errShortEncoding
	}
	return string(b[:length]), b[length:], nil
}

var errShortEncoding = errShort{}

type errShort struct{}

func (errShort) Error() string { return "truncated canonical encoding" }
