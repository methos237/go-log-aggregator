package agent

import (
	"io/fs"
	"syscall"
)

// fileIDFromInfo extracts the device and inode numbers from a stat result.
// See fileid_linux.go for why this is per-GOOS; on darwin st_dev is signed.
func fileIDFromInfo(info fs.FileInfo) (FileID, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return FileID{}, false
	}
	return FileID{Dev: uint64(st.Dev), Ino: st.Ino}, true
}
