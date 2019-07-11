package main

import (
	"testing"

	"github.com/pingcap/check"
)

type joinTestSuite struct{}

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&joinTestSuite{})

func (s *joinTestSuite) TestReadCSV(c *check.C) {
	tbl1 := readCSVFileIntoTbl("./t/r0.tbl")
	tbl2 := readCSV("./t/r0.tbl")

	c.Assert(len(tbl2), check.Equals, len(tbl1))

	for i := 0; i < len(tbl2); i++ {
		row := tbl2[i]
		for j := 0; j < len(tbl1[i]); j++ {
			col := tbl1[i][j]
			compare := row
			c.Assert(col, check.Equals, string(compare[j]))
		}
	}
}

func (s *joinTestSuite) TestBuild(c *check.C) {
	data1 := readCSVFileIntoTbl("./t/r0.tbl")
	p1 := buildHashTable(data1, []int{0})
	data2 := readCSV("./t/r0.tbl")
	p2 := build(data2, []int{0})
	c.Assert(p1.Len(), check.Equals, p2.Len())

	iter := p1.NewIterator()

	for i := 0; i < p1.Len(); i++ {
		k, _ := iter.Next()
		v1 := p1.Get(k, [][]byte{})
		v2 := p2.Get(k, [][]byte{})
		for i := 0; i < len(v1); i++ {
			c.Assert(v1[i], check.BytesEquals, v2[i])
		}
	}
}

func (s *joinTestSuite) TestJoinExample(c *check.C) {
	for _, t := range []struct {
		f0       string
		f1       string
		offsets0 []int
		offsets1 []int
		sum      uint64
	}{
		// r0 join r0 on r0.col0 = r0.col1
		{"./t/r0.tbl", "./t/r0.tbl", []int{0}, []int{1}, 0x16CBF2D},
		// r0 join r1 on r0.col0 = r1.col0
		{"./t/r0.tbl", "./t/r1.tbl", []int{0}, []int{0}, 0xC1D73B},
		// r0 join r2 on r0.col0 = r2.col0
		{"./t/r0.tbl", "./t/r2.tbl", []int{0}, []int{0}, 0x1F235},
		// r0 join r1 on r0.col0 = r1.col0 and r0.col1 = r1.col1
		{"./t/r0.tbl", "./t/r1.tbl", []int{0, 1}, []int{0, 1}, 0},
		// r1 join r2 on r1.col0 = r2.col0
		{"./t/r1.tbl", "./t/r2.tbl", []int{0}, []int{0}, 0x18CDA},
		// r2 join r2 on r2.col0 = r2.col0 and r2.col1 = r2.col1
		{"./t/r2.tbl", "./t/r2.tbl", []int{0, 1}, []int{0, 1}, 0x5B385},
	} {
		c.Assert(JoinExample(t.f0, t.f1, t.offsets0, t.offsets1), check.Equals, t.sum)
	}
}

func (s *joinTestSuite) TestJoin(c *check.C) {
	for _, t := range []struct {
		f0       string
		f1       string
		offsets0 []int
		offsets1 []int
		sum      uint64
	}{
		// r0 join r0 on r0.col0 = r0.col1
		{"./t/r0.tbl", "./t/r0.tbl", []int{0}, []int{1}, 0x16CBF2D},
		// r0 join r1 on r0.col0 = r1.col0
		{"./t/r0.tbl", "./t/r1.tbl", []int{0}, []int{0}, 0xC1D73B},
		// r0 join r2 on r0.col0 = r2.col0
		{"./t/r0.tbl", "./t/r2.tbl", []int{0}, []int{0}, 0x1F235},
		// r0 join r1 on r0.col0 = r1.col0 and r0.col1 = r1.col1
		{"./t/r0.tbl", "./t/r1.tbl", []int{0, 1}, []int{0, 1}, 0},
		// r1 join r2 on r1.col0 = r2.col0
		{"./t/r1.tbl", "./t/r2.tbl", []int{0}, []int{0}, 0x18CDA},
		// r2 join r2 on r2.col0 = r2.col0 and r2.col1 = r2.col1
		{"./t/r2.tbl", "./t/r2.tbl", []int{0, 1}, []int{0, 1}, 0x5B385},
	} {
		c.Assert(Join(t.f0, t.f1, t.offsets0, t.offsets1), check.Equals, t.sum)
	}
}
