// +build !race

package ensure_test

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func indirect(f ensure.Fataler) {
	ensure.StringContains(f, "foo", "bar")
}

func TestIndirectStackTrace(t *testing.T) {
	var c capture
	indirect(&c)
	c.Contains(t, "github.com/facebookgo/ensure/ensure_no_race_test.go:12")
	c.Contains(t, "indirect")
	c.Contains(t, "github.com/facebookgo/ensure/ensure_no_race_test.go:17")
	c.Contains(t, "TestIndirectStackTrace")
	c.Contains(t, `expected substring "bar" was not found in "foo"`)
}
