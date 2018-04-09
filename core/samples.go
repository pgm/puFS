package core

import (
	"math/rand"
	"sort"
	"sync"
)

type Population struct {
	count   int
	samples []int
	mut     sync.Mutex
}

func NewPopulation(capacity int) *Population {
	return &Population{samples: make([]int, capacity)}
}

func (p *Population) Add(value int) {
	p.mut.Lock()
	if p.count < len(p.samples) {
		p.samples[p.count] = value
	} else {
		i := rand.Intn(p.count)
		if i <= len(p.samples) {
			p.samples[i] = value
		}
	}
	p.count++
	p.mut.Unlock()
}

func (p *Population) getSortedSamples() []int {
	p.mut.Lock()
	size := p.count
	if p.count > len(p.samples) {
		size = len(p.samples)
	}
	dest := make([]int, size)
	copy(dest, p.samples[:size])
	p.mut.Unlock()

	sort.Ints(dest)

	return dest
}

func (p *Population) Percentiles(percentiles []float32) ([]int, bool) {
	result := make([]int, len(percentiles))
	s := p.getSortedSamples()
	if len(s) <= 1 {
		return nil, false
	} else {
		for j := range result {
			i := int(float32(len(s))*percentiles[j]*100) / 100
			if i >= len(s) {
				i = len(s) - 1
			}
			result[j] = s[i]
		}
		return result, true
	}
}

func (p *Population) Count() int {
	p.mut.Lock()
	n := p.count
	p.mut.Unlock()
	return n
}

// func (p *Population) Median() (int, bool) {
// 	return p.Percentile(50)
// }
