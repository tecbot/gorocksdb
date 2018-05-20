package ensure_test

import (
	"errors"
	"regexp"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestNilErr(t *testing.T) {
	var c capture
	e := errors.New("foo")
	ensure.Err(&c, e, nil)
	c.Equal(t, "ensure_test.go:14: unexpected error: foo")
}

func TestMatchingError(t *testing.T) {
	var c capture
	e := errors.New("foo")
	ensure.Err(&c, e, regexp.MustCompile("bar"))
	c.Equal(t, "ensure_test.go:21: expected error: \"bar\" but got \"foo\"")
}

type typ struct {
	Answer int
}

func TestExtras(t *testing.T) {
	var c capture
	e := errors.New("foo")
	ensure.Err(
		&c,
		e,
		nil,
		map[string]int{"answer": 42},
		"baz",
		43,
		44.45,
		typ{Answer: 46},
	)
	c.Equal(t, `ensure_test.go:41: unexpected error: foo
(map[string]int) (len=1) {
 (string) (len=6) "answer": (int) 42
}
(string) (len=3) "baz"
(int) 43
(float64) 44.45
(ensure_test.typ) {
 Answer: (int) 46
}`)
}

func TestDeepEqualStruct(t *testing.T) {
	var c capture
	actual := typ{Answer: 41}
	expected := typ{Answer: 42}
	ensure.DeepEqual(&c, actual, expected)
	c.Equal(t, `ensure_test.go:58: expected these to be equal:
ACTUAL:
(ensure_test.typ) {
 Answer: (int) 41
}

EXPECTED:
(ensure_test.typ) {
 Answer: (int) 42
}`)
}

func TestDeepEqualString(t *testing.T) {
	var c capture
	ensure.DeepEqual(&c, "foo", "bar")
	c.Equal(t, `ensure_test.go:73: expected these to be equal:
ACTUAL:
(string) (len=3) "foo"

EXPECTED:
(string) (len=3) "bar"`)
}

func TestNotDeepEqualStruct(t *testing.T) {
	var c capture
	v := typ{Answer: 42}
	ensure.NotDeepEqual(&c, v, v)
	c.Equal(t, `ensure_test.go:85: expected two different values, but got the same:
(ensure_test.typ) {
 Answer: (int) 42
}`)
}

func TestSubsetStruct(t *testing.T) {
	var c capture
	ensure.Subset(&c, typ{}, typ{Answer: 42})
	c.Equal(t, `ensure_test.go:94: expected subset not found:
ACTUAL:
(ensure_test.typ) {
 Answer: (int) 0
}

EXPECTED SUBSET
(ensure_test.typ) {
 Answer: (int) 42
}`)
}

func TestUnexpectedNilErr(t *testing.T) {
	var c capture
	ensure.Err(&c, nil, regexp.MustCompile("bar"))
	c.Equal(t, "ensure_test.go:109: expected error: \"bar\" but got a nil error")
}

func TestNilString(t *testing.T) {
	var c capture
	ensure.Nil(&c, "foo")
	c.Equal(t, "ensure_test.go:115: expected nil value but got: (string) (len=3) \"foo\"")
}

func TestNilInt(t *testing.T) {
	var c capture
	ensure.Nil(&c, 1)
	c.Equal(t, "ensure_test.go:121: expected nil value but got: (int) 1")
}

func TestNilStruct(t *testing.T) {
	var c capture
	ensure.Nil(&c, typ{})
	c.Equal(t, `ensure_test.go:127: expected nil value but got:
(ensure_test.typ) {
 Answer: (int) 0
}`)
}

func TestNonNil(t *testing.T) {
	var c capture
	ensure.NotNil(&c, nil)
	c.Equal(t, `ensure_test.go:136: expected a value but got nil`)
}

func TestStringContains(t *testing.T) {
	var c capture
	ensure.StringContains(&c, "foo", "bar")
	c.Equal(t, "ensure_test.go:142: expected substring \"bar\" was not found in \"foo\"")
}

func TestStringDoesNotContain(t *testing.T) {
	var c capture
	ensure.StringDoesNotContain(&c, "foo", "o")
	c.Equal(t, "ensure_test.go:148: substring \"o\" was not supposed to be found in \"foo\"")
	if log {
		t.Log("foo")
	}
}

func TestExpectedNilErr(t *testing.T) {
	var c capture
	ensure.Err(&c, nil, nil)
	c.Equal(t, "")
}

func TestNilErrUsingNil(t *testing.T) {
	var c capture
	e := errors.New("foo")
	ensure.Nil(&c, e)
	c.Equal(t, "ensure_test.go:164: unexpected error: foo")
}

func TestTrue(t *testing.T) {
	var c capture
	ensure.True(&c, false)
	c.Equal(t, `ensure_test.go:170: expected true but got false`)
}

func TestSameElementsIntAndInterface(t *testing.T) {
	ensure.SameElements(t, []int{1, 2}, []interface{}{2, 1})
}

func TestSameElementsLengthDifference(t *testing.T) {
	var c capture
	ensure.SameElements(&c, []int{1, 2}, []interface{}{1})
	c.Equal(t, `ensure_test.go:180: expected same elements but found slices of different lengths:
ACTUAL:
([]int) (len=2 cap=2) {
 (int) 1,
 (int) 2
}
EXPECTED
([]interface {}) (len=1 cap=1) {
 (int) 1
}`)
}

func TestSameElementsRepeated(t *testing.T) {
	var c capture
	ensure.SameElements(&c, []int{1, 2}, []interface{}{1, 1})
	c.Equal(t, `ensure_test.go:195: missing expected element:
ACTUAL:
([]int) (len=2 cap=2) {
 (int) 1,
 (int) 2
}
EXPECTED:
([]interface {}) (len=2 cap=2) {
 (int) 1,
 (int) 1
}
MISSING ELEMENT
(int) 1`)
}

func TestFalse(t *testing.T) {
	var c capture
	ensure.False(t, false)
	ensure.False(&c, true)
	c.Equal(t, `ensure_test.go:214: expected false but got true`)
}

func TestPanicDeepEqualNil(t *testing.T) {
	defer ensure.PanicDeepEqual(t, "can't pass nil to ensure.PanicDeepEqual")
	ensure.PanicDeepEqual(t, nil)
}

func TestPanicDeepEqualSuccess(t *testing.T) {
	defer ensure.PanicDeepEqual(t, 1)
	panic(1)
}

func TestPanicDeepEqualFailure(t *testing.T) {
	var c capture
	func() {
		defer ensure.PanicDeepEqual(&c, 1)
		panic(2)
	}()
	c.Matches(t, `TestPanicDeepEqualFailure((.func1)?)
expected these to be equal:
ACTUAL:
\(int\) 2

EXPECTED:
\(int\) 1`)
}

func TestMultiLineStringContains(t *testing.T) {
	var c capture
	ensure.StringContains(&c, "foo\nbaz", "bar")
	c.Equal(t, `ensure_test.go:245: expected substring was not found:
EXPECTED SUBSTRING:
bar
ACTUAL:
foo
baz`)
}
