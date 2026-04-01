//go:build !darwin

package group

import "io"

func newAutoSelectorPlatformWatcher(selector *AutoSelector) (io.Closer, error) {
	return nil, nil
}
