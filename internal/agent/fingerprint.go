package agent

import (
	"errors"
	"hash/fnv"
	"io"
	"os"
)

// headFingerprintLen is the fixed number of bytes hashed to fingerprint a
// file's head.
//
// It must be fixed rather than min(headFingerprintLen, size): a fingerprint
// taken over a variable length changes every time a short file grows, which
// would report truncation on an ordinary append — the single easiest way to
// get this check wrong.
const headFingerprintLen = 256

// fingerprintHead hashes the first headFingerprintLen bytes of f.
//
// It uses ReadAt rather than Read/Seek so this never disturbs f's current
// read offset: the tail source tracks that offset itself, separately, and a
// fingerprint that moved it would silently skip or re-emit lines.
//
// A file shorter than headFingerprintLen returns the zero Fingerprint:
// there is not enough content to identify, and returning "not
// fingerprinted" is the honest answer, so callers fall back to inode and
// size for it instead of hashing a short prefix that would look different
// on every ordinary append as the file grows past this call and the next.
//
// FNV-1a (hash/fnv, 64-bit) is a non-cryptographic hash. That is deliberate:
// this identifies a file's content well enough to notice a truncate-and-
// rewrite race, it does not need to resist an adversary crafting a
// collision, and there is no reason to pay for something like SHA-256 here.
// Do not "upgrade" this later without a real threat model that needs it.
func fingerprintHead(f *os.File) (Fingerprint, error) {
	buf := make([]byte, headFingerprintLen)
	n, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return Fingerprint{}, err
	}
	if n < headFingerprintLen {
		return Fingerprint{}, nil
	}

	h := fnv.New64a()
	_, _ = h.Write(buf) // hash.Hash.Write on an in-memory buffer never errors.

	return Fingerprint{Len: int64(n), Hash: h.Sum64()}, nil
}
