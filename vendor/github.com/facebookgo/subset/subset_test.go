package subset

import (
	"testing"
)

type test struct {
	Name string
	A    interface{}
	B    interface{}
}

type sample struct {
	Answer  int
	Name    string
	Child   *sample
	private string
}

var (
	subsetTests = []test{
		test{"Integers", 1, 1},
		test{"Strings", "a", "a"},
		test{"Maps",
			map[string]string{"foo": "bar"},
			map[string]string{"foo": "bar"}},
		test{"Maps subset",
			map[string]string{"foo": "bar"},
			map[string]string{"foo": "bar", "answer": "42"}},
		test{"Nil map", nil, map[string]string{"foo": "bar"}},
		test{"Structs", sample{Answer: 1}, sample{Answer: 1}},
		test{"Struct subset", sample{Answer: 1}, sample{Answer: 1, Name: "a"}},
		test{"Nil pointer", sample{}, sample{Child: &sample{}}},
	}
	notSubsetTests = []test{
		test{"Integer", 1, 2},
		test{"Integers of different types", uint(1), int(1)},
		test{"Maps not subset",
			map[string]string{"foo": "bar", "answer": "42"},
			map[string]string{"foo": "bar"}},
		test{"Structs", sample{Answer: 1}, sample{Answer: 2}},
		test{"Struct subset",
			sample{Answer: 1, Name: "b"},
			sample{Answer: 1, Name: "a"}},
	}
)

func TestSubsets(t *testing.T) {
	for _, d := range subsetTests {
		if !Check(d.A, d.B) {
			t.Errorf("Was expecting \"%s\" Check(%v, %v) == true.", d.Name, d.A, d.B)
		}
	}
}

func TestNotSubsets(t *testing.T) {
	for _, d := range notSubsetTests {
		if Check(d.A, d.B) {
			t.Errorf("Was expecting %s Check(%v, %v) == false.", d.Name, d.A, d.B)
		}
	}
}
