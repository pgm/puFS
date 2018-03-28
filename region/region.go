package region

import (
	"fmt"
	"sort"
)

type Region struct {
	Start int64
	End   int64
}

type Mask struct {
	regions []Region
}

func New() *Mask {
	return &Mask{}
}

func makeCmp(m *Mask, start int64) func(int) bool {
	return (func(i int) bool {
		return m.regions[i].Start >= start
	})
}

func (m *Mask) GetMissing(start int64, end int64) []Region {
	result := make([]Region, 0, 10)

	cmp := makeCmp(m, start)
	i := sort.Search(len(m.regions), cmp)
	if i > 0 && m.regions[i-1].End >= start {
		start = m.regions[i-1].End
		i++
	} else if i < len(m.regions) && m.regions[i].Start == start {
		start = m.regions[i].End
		i++
	}

	for {
		if i >= len(m.regions) {
			break
		}

		thisEnd := end
		if m.regions[i].Start < end {
			thisEnd = m.regions[i].Start
		}

		result = append(result, Region{start, thisEnd})
		start = m.regions[i].End
		i++
	}

	if start < end {
		result = append(result, Region{start, end})
	}

	return result
}

func (m *Mask) addDisjoint(start int64, end int64) {
	fmt.Printf("addDisjoint(%d, %d)\n", start, end)
	if start == end {
		return
	}
	if end < start {
		panic("invalid range")
	}
	cmp := makeCmp(m, start)
	i := sort.Search(len(m.regions), cmp)

	// i is the index where it would be inserted.
	// now, check if we align with the adjacent elements
	mergeLeft := i > 0 && m.regions[i-1].End == start
	mergeRight := i < len(m.regions) && m.regions[i].Start == end

	if mergeLeft && !mergeRight {
		m.regions[i-1].End = end
	} else if mergeRight && !mergeLeft {
		m.regions[i].Start = start
	} else if mergeRight && mergeLeft {
		// merge both
		m.regions[i-1].End = m.regions[i].End
		m.regions = append(m.regions[:i], m.regions[i+1:]...)
	} else {
		m.regions = append(append(m.regions[:i], Region{start, end}), m.regions[i:]...)
	}
}

func (m *Mask) Add(start int64, end int64) {
	disjointRegions := m.GetMissing(start, end)
	for _, r := range disjointRegions {
		m.addDisjoint(r.Start, r.End)
	}
}
