//go:build !linux && !darwin

package agent

import "io/fs"

// fileIDFromInfo has no implementation on this platform.
//
// The agent tails files by inode, and a platform where it cannot read one cannot
// detect rotation correctly. Reporting false here makes that a visible
// configuration failure at startup rather than a tail source that silently reads
// the wrong file after the first rotation. Linux is the deployment target and
// darwin is the development one; anything else is unsupported on purpose.
func fileIDFromInfo(fs.FileInfo) (FileID, bool) { return FileID{}, false }
