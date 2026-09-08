package agent

import (
	"io/fs"
	"syscall"
)

// fileIDFromInfo extracts the device and inode numbers from a stat result.
//
// There is no portable accessor for these: os.SameFile compares two FileInfos
// but cannot hand back an identity, and the checkpoint store needs numbers it can
// write to a file and compare after a restart. So this is per-GOOS, split into
// one file per platform rather than one file with casts, because syscall.Stat_t
// field widths differ and a conversion that is required on one platform is
// flagged as redundant by unconvert on the other.
//
// The second return is false when the FileInfo did not come from the filesystem,
// which happens for the fake FileInfos in tests and for io/fs implementations
// that are not the OS.
func fileIDFromInfo(info fs.FileInfo) (FileID, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return FileID{}, false
	}
	return FileID{Dev: st.Dev, Ino: st.Ino}, true
}
