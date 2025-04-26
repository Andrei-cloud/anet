// Package anet_test provides tests and examples for the anet package.
// It demonstrates proper usage patterns and validates core functionality.
//
// The test suite includes:
//   - Message framing protocol validation
//   - Connection pool management
//   - Broker request/response handling
//   - Error handling and edge cases
//   - Performance under concurrent load
//   - Graceful shutdown behavior
//
// Examples in this package demonstrate:
//   - Basic client/server communication
//   - Connection pooling configuration
//   - Broker setup and usage
//   - Error handling patterns
//   - Timeout and cancellation
//   - Graceful shutdown
package anet_test

import (
	"testing"

	"github.com/andrei-cloud/anet"
)

// TestPackageImport verifies the package can be imported correctly.
// This is a basic sanity check and serves as a template for new tests.
//
//nolint:all
func TestPackageImport(t *testing.T) {
	t.Parallel() // Tests can run in parallel

	// Verify package can be imported by referencing a package-level symbol
	_ = anet.ErrQuit
}
