package test

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	tapestry "tapestry/pkg"
	"testing"
	"time"
)

func TestFindRootOnRemoteNode1(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1", "3", "5", "7")
	fmt.Printf("length of tap %d\n", len(tap))
	tapestry.KillTapestries(tap[1], tap[2])                //Kill off two of them.
	next, _, _ := tap[0].FindRoot(tapestry.MakeID("2"), 0) //After killing 3 and 5, this should route to 7
	if next != tap[3].Node {
		t.Errorf("Failed to kill successfully")
	}
}

// test node get store remove, between remote nodes
func TestNodeGetStoreRemove(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "100", "456", "1234")
	tap[1].Store("look at this lad", []byte("an absolute unit"))
	result, _ := tap[2].Get("look at this lad")
	if !bytes.Equal(result, []byte("an absolute unit")) {
		t.Errorf("Get failed")
	}

	tap[1].Remove("look at this lad")
	result2, _ := tap[0].Get("look at this lad")
	if result2 != nil {
		t.Errorf("fail to remove value")
	}
}

// test find root with bad node
func TestFindRootOnRemoteNode2_BadNode(t *testing.T) {
	tap, _ := tapestry.Start(tapestry.MakeID("11"), 0, "")
	defer tapestry.KillTapestries(tap)

	badNode := tapestry.RemoteNode{tapestry.MakeID("21"), "abcd"}
	tap.Table.Add(badNode)

	root, err := tap.FindRootOnRemoteNode(tap.Node, badNode.ID)

	assert.Equal(t, err, nil)
	assert.Equal(t, root, tap.Node)
	assert.Equal(t, hasRoutingTableNode(tap, badNode), false)
}

// test find root with one better bad node returns self
func TestFindRootOnRemoteNode1_BadNode(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "10", "20")
	defer tapestry.KillTapestries(tap...)

	root, err := tap[0].FindRootOnRemoteNode(tap[0].Node, tap[1].Node.ID)
	assert.Equal(t, err, nil)
	assert.Equal(t, root, tap[1].Node)

	t2 := tap[1].Node.ID
	tapestry.KillTapestries(tap[1])
	time.Sleep(200 * time.Millisecond)
	root, err = tap[0].FindRootOnRemoteNode(tap[0].Node, t2)

	if err != nil {
		t.Errorf("%v", err)
	}

	assert.Equal(t, err, nil)
	assert.Equal(t, root, tap[0].Node)
	tapestry.KillTapestries(tap[0])

}

func TestPublish(t *testing.T) {
	tap1, _ := tapestry.Start(tapestry.MakeID("1"), 0, "")
	tap2, _ := tapestry.Start(tapestry.MakeID("2"), 0, tap1.Node.Address)
	defer tapestry.KillTapestries(tap1)
	defer tapestry.KillTapestries(tap2)

	_, err1 := tap1.Publish("hhh")
	assert.Equal(t, err1, nil)
	assert.Equal(t, len(tap1.LocationsByKey.Get("hhh")), 1)
	assert.Equal(t, len(tap2.LocationsByKey.Get("hhh")), 0)
	assert.Equal(t, tap1.LocationsByKey.Get("hhh")[0], tap1.Node)
}
