package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPopulation(t *testing.T) {
	require := require.New(t)
	p := NewPopulation(10)
	p.Add(5)
	p.Add(2)
	p.Add(3)
	p.Add(1)
	p.Add(-1)

	m, ok := p.Percentiles([]float32{0, 50, 100})
	require.True(ok)
	require.Equal([]int{-1, 2, 5}, m)
}
