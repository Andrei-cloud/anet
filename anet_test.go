// Package anet_test provides tests for the anet package.
package anet_test

import (
	"testing"

	"github.com/andrei-cloud/anet"
)

// TestPackageImport is a basic test that ensures the package can be imported.
// This file is kept for general integration tests that don't fit elsewhere.
//
//nolint:all
func TestPackageImport(t *testing.T) {
	// This test simply verifies that the package can be imported
	// and is a placeholder for any future general tests.
	t.Parallel()     // Add t.Parallel() to fix paralleltest linter warning
	_ = anet.ErrQuit // Reference something from the package to prevent import errors
}
