package test

import (
	"math/big"
	tapestry "tapestry/pkg"
	"testing"
)

func TestEnsureConsts(t *testing.T) {
	if tapestry.BASE != int(16) {
		t.Errorf("ID tests implemented for base 16 ids, current BASE is %v, re-run tests with correct BASE", tapestry.BASE)
	}
	if tapestry.DIGITS != int(40) {
		t.Errorf("ID tests implemented for 40-digit ids, current DIGITS is %v, re-run tests with correct DIGITS", tapestry.DIGITS)
	}
}

func TestRandom(t *testing.T) {
	for i := 0; i < 100; i++ {
		a := tapestry.RandomID()
		b := tapestry.RandomID()
		if a == b {
			t.Errorf("Two random IDs were the same: %v and %v", a, b)
		}
	}
}

func TestHash(t *testing.T) {
	cases := []string{"hello", "world", "Jonathan Mace", "Jeff Rasley",
		"Rodrigo Fonseca", "Tom Doeppner", "Cody Mello", "foo", "bar",
		"Brown university", "Brown University"}
	for _, c := range cases {
		a := tapestry.Hash(c)
		b := tapestry.Hash(c)
		if a != b {
			t.Errorf("Did not get same ID for %v: %v and %v", c, a, b)
		}

		for _, d := range cases {
			b := tapestry.Hash(d)
			if c != d && a == b {
				t.Errorf("Got same ID for different strings: %v, %v: %v", c, d, b)
			}
		}
	}
}

func TestDeterministic(t *testing.T) {
	cases := []string{"hello", "world", "Jonathan Mace", "Jeff Rasley",
		"Rodrigo Fonseca", "Tom Doeppner", "Cody Mello", "foo", "bar",
		"Brown university", "Brown University"}
	expected := []string{
		"AAF4C61DDCC5E8A2DABEDE0F3B482CD9AEA9434D",
		"7C211433F02071597741E6FF5A8EA34789ABBF43",
		"F909A9200B27BF625E8A0BA2E06758AEEC254CAD",
		"CF316664F024C6DC09025736055948AA73E54839",
		"8643ADD0432D58CA043FFF23C87FDF07F460B8FA",
		"19EA76F5E3B30953077D5747818D4692CF54847E",
		"519BFDD559CB03792ECD7565AEA263D08F3CF528",
		"0BEEC7B5EA3F0FDBC95D0DD47F3C5BC275DA8A33",
		"62CDB7020FF920E5AA642C3D4066950DD1F01F4D",
		"E4F3C8B0B3AE75E720AF1322F045FFD51031A6EE",
		"54D31843077B586455EAF042590739FBDAE8AA84",
	}
	for i, c := range cases {
		s := tapestry.Hash(c).String()
		if expected[i] != s {
			t.Errorf("%v did not produce expected ID %v, instead %v", c, expected[i], s)
		}
	}
}

func TestParse(t *testing.T) {
	cases := []string{"hello", "world", "Jonathan Mace", "Jeff Rasley",
		"Rodrigo Fonseca", "Tom Doeppner", "Cody Mello", "foo", "bar",
		"Brown university", "Brown University"}
	expected := []string{
		"AAF4C61DDCC5E8A2DABEDE0F3B482CD9AEA9434D",
		"7C211433F02071597741E6FF5A8EA34789ABBF43",
		"F909A9200B27BF625E8A0BA2E06758AEEC254CAD",
		"CF316664F024C6DC09025736055948AA73E54839",
		"8643ADD0432D58CA043FFF23C87FDF07F460B8FA",
		"19EA76F5E3B30953077D5747818D4692CF54847E",
		"519BFDD559CB03792ECD7565AEA263D08F3CF528",
		"0BEEC7B5EA3F0FDBC95D0DD47F3C5BC275DA8A33",
		"62CDB7020FF920E5AA642C3D4066950DD1F01F4D",
		"E4F3C8B0B3AE75E720AF1322F045FFD51031A6EE",
		"54D31843077B586455EAF042590739FBDAE8AA84",
	}
	for i, c := range cases {
		id := tapestry.Hash(c)
		parsed, err := tapestry.ParseID(expected[i])
		if err != nil {
			t.Error(err)
		}
		if parsed != id {
			t.Errorf("ParseID(%v) != Hash(%v) (%v)", expected[i], c, id)
		}
		if parsed.String() != expected[i] {
			t.Errorf("%v.String() != %v", parsed, expected[i])
		}
	}
}

func TestBigID(t *testing.T) {
	cases := []struct {
		str    string
		expect int64
	}{{"0000000000000000000000000000000000000000", 0},
		{"0000000000000000000000000000000000000001", 1},
		{"0000000000000000000000000000000000000010", 16},
		{"0000000000000000000000000000000000011170", 70000},
	}

	for _, c := range cases {
		id, _ := tapestry.ParseID(c.str)
		b := id.Big()
		expect := big.NewInt(c.expect)
		if b.Cmp(expect) != 0 {
			t.Errorf("'%v'.big() != %v (was %v)", id, expect, b)
		}
	}
}

func checkCloser(a, b, c string, closer bool, t *testing.T) {
	ida, _ := tapestry.ParseID(a)
	idb, _ := tapestry.ParseID(b)
	idc, _ := tapestry.ParseID(c)
	if ida.Closer(idb, idc) != closer {
		t.Errorf("Expected '%v'.Closer('%v', '%v') == %v", a, b, c, closer)
	}
}

func TestCloser(t *testing.T) {
	a, _ := tapestry.ParseID("0000000000000000000001000000000000000000")
	b, _ := tapestry.ParseID("0000000000000000000000100000000000000000")
	c, _ := tapestry.ParseID("0000000000000000000000010000000000000000")

	if !a.Closer(b, c) {
		t.Errorf("Expected '%v'.Closer('%v','%v)", a, b, c)
	}
	if a.Closer(c, b) {
		t.Errorf("Expected !'%v'.Closer('%v','%v)", a, c, b)
	}
	if !b.Closer(c, a) {
		t.Errorf("Expected '%v'.Closer('%v','%v)", b, c, a)
	}
	if b.Closer(a, c) {
		t.Errorf("Expected !'%v'.Closer('%v','%v)", b, a, c)
	}
	if !c.Closer(b, a) {
		t.Errorf("Expected '%v'.Closer('%v','%v)", c, b, a)
	}
	if c.Closer(a, b) {
		t.Errorf("Expected !'%v'.Closer('%v','%v)", c, a, b)
	}

	checkCloser("0000000000000000000001000000000000000000",
		"0000000000000000000001000000000000000000",
		"0000000000000000000001000000000000000000",
		false, t,
	)
	checkCloser("1000000000000000000000000000000000000000",
		"0FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
		"1000000000000000000000000000000000000002",
		true, t,
	)
}

func TestDigitDistance(t *testing.T) {
	cases := []struct {
		me, a, b string
		expected bool
	}{{"hello", "world", "Jonathan Mace", true},
		{"hello", "Jonathan Mace", "world", false},
		{"Brown University", "Brown university", "bar", false},
		{"Brown University", "bar", "Brown university", true},
		{"bar", "Brown University", "Brown university", true},
		{"bar", "Brown university", "Brown University", false},
		{"hello", "world", "world", false},
		{"hello", "Jonathan Mace", "Jonathan Mace", false},
	}

	for _, c := range cases {
		idme := tapestry.Hash(c.me)
		ida := tapestry.Hash(c.a)
		idb := tapestry.Hash(c.b)
		closer := idme.Closer(ida, idb)
		expected := c.expected
		if closer != expected {
			t.Errorf("Expected (\"%v\".Closer(\"%v\", \"%v\") == %v, but was %v.\nidme=%v (%v)\nida= %v (%v)\nidb= %v (%v)", c.me, c.a, c.b, expected, closer, idme, c.me, ida, c.a, idb, c.b)
		}
	}
}

func TestDigitsSet(t *testing.T) {
	for i := 0; i < 16; i++ {
		if int(tapestry.Digit(i)) != i {
			t.Errorf("Digit %v wasn't %v", tapestry.Digit(i), i)
		}
	}
}

func TestIDSimple(t *testing.T) {
	var d [tapestry.DIGITS]tapestry.Digit
	for i := 0; i < tapestry.DIGITS; i++ {
		d[i] = tapestry.Digit(i)
	}
	for i := 0; i < tapestry.DIGITS; i++ {
		if int(d[i]) != i {
			t.Errorf("%v != %v", d[i], i)
		}
	}
}
