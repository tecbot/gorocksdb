package gorocksdb

import "C"

func NewMockMergeOperator(fm func(key, existingValue []byte, operands [][]byte) ([]byte, bool), pm func(key, leftOperand, rightOperand []byte) ([]byte, bool)) MergeOperator {
	return &mockMergeOperator{fm, pm}
}

type mockMergeOperator struct {
	fullMerge    func(key, existingValue []byte, operands [][]byte) ([]byte, bool)
	partialMerge func(key, leftOperand, rightOperand []byte) ([]byte, bool)
}

func (m *mockMergeOperator) Name() string   { return "gorocksdb.test" }
func (m *mockMergeOperator) CName() *C.char { return C.CString("gorocksdb.test") }
func (m *mockMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return m.fullMerge(key, existingValue, operands)
}
func (m *mockMergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return m.partialMerge(key, leftOperand, rightOperand)
}
