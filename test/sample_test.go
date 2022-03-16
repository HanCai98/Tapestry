package test

import (
	"bytes"
	"fmt"
	tapestry "tapestry/pkg"
	"testing"
)

func TestSampleTapestrySetup(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1", "3", "5", "7") //Make a tapestry with these ids
	fmt.Printf("length of tap %d\n", len(tap))
	tapestry.KillTapestries(tap[1], tap[2])                //Kill off two of them.
	next, _, _ := tap[0].FindRoot(tapestry.MakeID("2"), 0) //After killing 3 and 5, this should route to 7
	if next != tap[3].Node {
		t.Errorf("Failed to kill successfully")
	}

}

func TestSampleTapestrySearch(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "100", "456", "1234") //make a sample tap
	tap[1].Store("look at this lad", []byte("an absolute unit"))
	result, _ := tap[0].Get("look at this lad")           //Store a KV pair and try to fetch it
	if !bytes.Equal(result, []byte("an absolute unit")) { //Ensure we correctly get our KV
		t.Errorf("Get failed")
	}
}

func TestSampleTapestryAddNodes(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1", "5", "9")
	node8, tap, _ := tapestry.AddOne("8", tap[0].Node.Address, tap) //Add some tap nodes after the initial construction
	_, tap, _ = tapestry.AddOne("12", tap[0].Node.Address, tap)

	next, _, _ := tap[1].FindRoot(tapestry.MakeID("7"), 0)
	if node8.Node != next {
		t.Errorf("Addition of node failed")
	}
}
