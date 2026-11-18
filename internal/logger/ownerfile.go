// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Hive Computing Services SA

package logger

import (
	"fmt"
	"os"
)

// OpenOwnerOnly opens path for appending, creating it with mode 0600, for a
// log whose lines name tenants and keys (the audit and decision logs): only
// the router's own user may read it. A regular file created earlier with a
// wider mode is narrowed to 0600; anything else (/dev/stdout, a pipe, a
// terminal) is left alone. A narrowing that fails is returned with the open
// file, which is still usable.
func OpenOwnerOnly(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if fi, err := f.Stat(); err == nil && fi.Mode().IsRegular() && fi.Mode().Perm() != 0o600 {
		if err := f.Chmod(0o600); err != nil {
			return f, fmt.Errorf("restrict %s to mode 0600: %w", path, err)
		}
	}
	return f, nil
}
