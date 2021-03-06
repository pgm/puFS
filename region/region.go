package region

import (
	"fmt"
	"log"
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
	// origStart := start
	// origEnd := end
	if end <= start {
		panic(fmt.Sprintf("GetMissing called on invalid range: start=%d, end=%d", start, end))
	}
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

		if thisEnd > start {
			result = append(result, Region{start, thisEnd})
		} else if thisEnd <= start {
			break
			//			panic(fmt.Sprintf("GetMissing failure: origStart=%d, origEnd=%d, start=%d, thisEnd=%d, regions=%v\n", origStart, origEnd, start, thisEnd, m.regions))
		}

		start = m.regions[i].End
		i++
	}

	if start < end {
		result = append(result, Region{start, end})
	}

	return result
}

func (m *Mask) addDisjoint(start int64, end int64) {
	if start == end {
		return
	}
	if end < start {
		panic(fmt.Sprintf("invalid range: (%d, %d)", start, end))
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
		newRegions := append(m.regions, Region{-1, -1})
		copy(newRegions[i+1:], newRegions[i:])
		newRegions[i] = Region{start, end}
		m.regions = newRegions
		// prefix := append(m.regions[:i], Region{start, end})
		// suffix := m.regions[i:]
		// fmt.Printf("prefix: %v\n", prefix)
		// fmt.Printf("suffix: %v\n", suffix)
		// fmt.Printf("m.regions= %v, m.regions[0:] = %v\n", m.regions, m.regions[0:])
		// m.regions = append(prefix, suffix...)
	}
}

func (m *Mask) Add(start int64, end int64) {
	//	fmt.Printf("m.Add(%d,%d) // %p\n", start, end, m)
	if end <= start {
		panic(fmt.Sprintf("Attempted add of invalid region: start=%d, end=%d", start, end))
	}
	disjointRegions := m.GetMissing(start, end)
	for _, r := range disjointRegions {
		if r.End < r.Start {
			panic(fmt.Sprintf("GetMissing(%d, %d) returned a segment: (%d, %d)", start, end, r.Start, r.End))
		}
		m.addDisjoint(r.Start, r.End)
	}
	m.TotalLength()
}

func (m *Mask) Validate() {
	for i, r := range m.regions {
		if i > 0 {

			prev := m.regions[i-1]
			if prev.End > r.Start {
				panic("Regions are not disjoint")
			}
			if prev.End == r.Start {
				log.Printf("Regions are contigious")
			}
		}
	}
}

func (m *Mask) TotalLength() int64 {
	total := int64(0)
	for _, r := range m.regions {
		total += r.End - r.Start
	}
	m.Validate()
	return total
}

func (m *Mask) Count() int {
	return len(m.regions)
}

func (m *Mask) GetFirstMissingRegion(start int64, end int64) *Region {
	regions := m.GetMissing(start, end)
	if len(regions) == 0 {
		return nil
	} else {
		r := regions[0]
		return &r
	}
}

/* returns the min start which is greater or equal to the provided position */
func (m *Mask) GetNextStart(position int64, maxValue int64) int64 {
	minStart := maxValue
	for _, r := range m.regions {
		if position <= r.Start {
			if minStart > r.Start {
				minStart = r.Start
			}
		}
	}
	return minStart
}
