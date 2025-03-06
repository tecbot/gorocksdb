package gorocksdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	opts := NewDefaultOptions()
	defer opts.Destroy()

	// Test setting max bg jobs
	assert.Equal(t, 2, opts.GetMaxBackgroundJobs())
	opts.SetMaxBackgroundJobs(10)
	assert.Equal(t, 10, opts.GetMaxBackgroundJobs())

	// Test setting max bg compactions
	assert.Equal(t, uint32(1), opts.GetMaxSubcompactions())
	opts.SetMaxSubcompactions(9)
	assert.Equal(t, uint32(9), opts.GetMaxSubcompactions())
}
