// Package version exposes build metadata stamped in at link time.
//
// Values are overridden with -ldflags "-X ...". They stay as "dev"/"none" for
// `go run`, which is how you can tell a local build from a released one.
package version

import (
	"fmt"
	"runtime"
)

var (
	// Version is the semantic version or git describe output.
	Version = "dev"
	// Commit is the git SHA the binary was built from.
	Commit = "none"
	// BuildDate is an RFC3339 timestamp.
	BuildDate = "unknown"
)

// String renders a single-line summary for --version output.
func String(binary string) string {
	return fmt.Sprintf("%s %s (commit %s, built %s, %s)",
		binary, Version, Commit, BuildDate, runtime.Version())
}
