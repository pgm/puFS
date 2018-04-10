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

func TestSample(t *testing.T) {
	require := require.New(t)

	m := New()
	// m.Add(0, 204800)
	// m.Add(1024000, 1228800)
	// m.Add(2048000, 2252800)
	// m.Add(3072000, 3276800)
	// m.Add(4096000, 4300800)
	// m.Add(5120000, 5324800)
	// m.Add(6144000, 6348800)
	m.Add(7168000, 7372800)
	m.Add(7782400, 7987200)
	m.Add(8806400, 9011200)
	// m.Add(9830400, 10035200)
	// m.Add(11059200, 11264000)
	// m.Add(12083200, 12288000)
	// m.Add(13107200, 13312000)
	// m.Add(14131200, 54229441)
	regions := m.GetMissing(7168000, 7372800)
	// origStart=7168000, origEnd=7372800, start=7372800, thisEnd=7372800, regions=[]
	require.Empty(regions)
}
func TestSample2(t *testing.T) {
	require := require.New(t)

	m := New()

	// m.Add(0, 204800)
	// m.Add(1024000, 1228800)
	// m.Add(2252800, 2457600)
	// m.Add(3276800, 3481600)
	// m.Add(4505600, 4710400)
	// m.Add(5734400, 5939200)
	// m.Add(6758400, 6963200)
	m.Add(7987200, 8192000)
	m.Add(9011200, 9216000)
	m.Add(10240000, 10444800)
	// m.Add(11468800, 11673600)
	// m.Add(12288000, 12492800)
	// m.Add(13312000, 13516800)
	// m.Add(14336000, 14745600)
	// m.Add(15564800, 15769600)
	// m.Add(16588800, 16998400)
	// m.Add(17817600, 18022400)
	// m.Add(18841600, 19251200)
	// m.Add(20070400, 20275200)
	// m.Add(21094400, 23552000)
	// m.Add(24166400, 24371200)
	// m.Add(25190400, 71475200)
	// m.Add(71680000, 71884800)
	// m.Add(72499200, 97894400)
	// r Read [ID=0xb Node=0x794 Uid=1944976379 Gid=1594166068 Pid=71570] 0x47 4096 @0x7b0000 dir=false fl=0 lock=0 ffl=OpenReadOnly:

	// GetMissing failure: origStart=7987200, origEnd=8192000, start=9216000, thisEnd=8192000, regions=[
	// 	{0 204800} {1024000 1228800} {2252800 2457600} {3276800 3481600} {4505600 4710400} {5734400 5939200} {6758400 6963200} {7987200 8192000} {9011200 9216000} {10240000 10444800} {11468800 11673600} {12288000 12492800} {13312000 13516800} {14336000 14745600} {15564800 15769600} {16588800 16998400} {17817600 18022400} {18841600 19251200} {20070400 20275200} {21094400 23552000} {24166400 24371200} {25190400 71475200} {71680000 71884800} {72499200 97894400}]
	regions := m.GetMissing(7987200, 8192000)
	// origStart=7168000, origEnd=7372800, start=7372800, thisEnd=7372800, regions=[]
	require.Empty(regions)

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
