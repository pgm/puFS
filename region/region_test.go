package region

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnds(t *testing.T) {
	require := require.New(t)

	m := New()
	rs := m.GetMissing(0, 10)
	require.Equal("0-10", rString(rs))

	m.Add(0, 1)
	rs = m.GetMissing(0, 10)
	require.Equal("1-10", rString(rs))

	m.Add(9, 10)
	rs = m.GetMissing(0, 10)
	require.Equal("1-9", rString(rs))

	m.Add(1, 2)
	rs = m.GetMissing(0, 10)
	require.Equal("2-9", rString(rs))

	m.Add(8, 9)
	rs = m.GetMissing(0, 10)
	require.Equal("2-8", rString(rs))
}

func TestOverlaps(t *testing.T) {
	require := require.New(t)

	m := New()
	rs := m.GetMissing(10, 20)
	require.Equal("10-20", rString(rs))

	m.Add(15, 16)
	rs = m.GetMissing(10, 20)
	require.Equal("10-15 16-20", rString(rs))

	m.Add(14, 16)
	rs = m.GetMissing(10, 20)
	require.Equal("10-14 16-20", rString(rs))

	m.Add(14, 17)
	rs = m.GetMissing(10, 20)
	require.Equal("10-14 17-20", rString(rs))

	m.Add(13, 19)
	rs = m.GetMissing(10, 20)
	require.Equal("10-13 19-20", rString(rs))
}

func TestMiddle(t *testing.T) {
	require := require.New(t)

	m := New()
	rs := m.GetMissing(0, 10)
	require.Equal("0-10", rString(rs))

	m.Add(1, 2)
	rs = m.GetMissing(0, 10)
	require.Equal("0-1 2-10", rString(rs))

	m.Add(8, 9)
	rs = m.GetMissing(0, 10)
	require.Equal("0-1 2-8 9-10", rString(rs))

	m.Add(2, 8)
	rs = m.GetMissing(0, 10)
	require.Equal("0-1 9-10", rString(rs))

}

func rString(rs []Region) string {
	var s []string
	for i := range rs {
		s = append(s, fmt.Sprintf("%d-%d", rs[i].Start, rs[i].End))
	}

	return strings.Join(s, " ")
}
