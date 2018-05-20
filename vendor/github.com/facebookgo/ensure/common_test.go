package ensure_test

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/facebookgo/ensure"
)

var log = os.Getenv("ENSURE_LOG") == "1"

type capture struct {
	bytes.Buffer
}

func (c *capture) Fatal(a ...interface{}) {
	fmt.Fprint(&c.Buffer, a...)
}

var equalPrefix = strings.Repeat("\b", 20)

func (c *capture) Equal(t testing.TB, expected string) {
	// trim the deleteSelf '\b' prefix
	actual := strings.TrimLeft(c.String(), "\b")
	ensure.DeepEqual(t, actual, expected)
	if log && expected != "" {
		t.Log(equalPrefix, expected)
	}
}

func (c *capture) Contains(t testing.TB, suffix string) {
	ensure.StringContains(t, c.String(), suffix)
	if log && suffix != "" {
		t.Log(equalPrefix, suffix)
	}
}

func (c *capture) Matches(t testing.TB, pattern string) {
	re := regexp.MustCompile(pattern)
	s := c.String()
	ensure.True(t, re.MatchString(s), s, "does not match pattern", pattern)
}
