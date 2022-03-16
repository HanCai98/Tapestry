package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	tapestry "tapestry/pkg"
	"testing"
	"time"
)

func TestNodeIDAndAddress(t *testing.T) {
	t1, _ := tapestry.Start(tapestry.MakeID("21"), 0, "")
	defer tapestry.KillTapestries(t1)

	fmt.Printf("t1 ID: %v", t1.ID())
	fmt.Printf("t1 Address: %v", t1.Addr())

}

// test Multicast, data transfer in multicast
func TestMulticast1(t *testing.T) {
	t1, _ := tapestry.Start(tapestry.MakeID("21"), 0, "")
	t1.Publish("data")
	t2, _ := tapestry.Start(tapestry.MakeID("11"), 0, t1.Node.Address)
	defer tapestry.KillTapestries(t1, t2)

	time.Sleep(200 * time.Millisecond)
	node, err := t2.Lookup("data")

	assert.Equal(t, err, nil)
	assert.Equal(t, len(node), 1)
	assert.Equal(t, node[0], t1.Node)
}

// test Multicast, bad node removed from routing table
func TestMulticast2(t *testing.T) {
	t1, _ := tapestry.Start(tapestry.MakeID("11"), 0, "")
	t2, _ := tapestry.Start(tapestry.MakeID("12"), 0, t1.Node.Address)
	t3, _ := tapestry.Start(tapestry.MakeID("123"), 0, "")
	defer tapestry.KillTapestries(t1, t2, t3)

	time.Sleep(200 * time.Millisecond)
	badnode := tapestry.RemoteNode{tapestry.MakeID("1234"), "abcd"}
	t2.Table.Add(t3.Node)
	t3.Table.Add(badnode)

	assert.Equal(t, hasRoutingTableNode(t3, badnode), true)

	t4, _ := tapestry.Start(tapestry.MakeID("0"), 0, "")
	t1.AddNode(t4.Node)

	assert.Equal(t, hasRoutingTableNode(t3, badnode), false)
}

// test Multicast, bad node not in neighbour
func TestMulticast3(t *testing.T) {
	t1, _ := tapestry.Start(tapestry.MakeID("11"), 0, "")
	t2, _ := tapestry.Start(tapestry.MakeID("12"), 0, t1.Node.Address)
	t3, _ := tapestry.Start(tapestry.MakeID("123"), 0, "")
	defer tapestry.KillTapestries(t1, t2, t3)

	time.Sleep(200 * time.Millisecond)
	badnode := tapestry.RemoteNode{tapestry.MakeID("1234"), "abcd"}
	t2.Table.Add(t3.Node)
	t3.Table.Add(badnode)

	t4, _ := tapestry.Start(tapestry.MakeID("0"), 0, "")
	neighbors, err := t1.AddNode(t4.Node)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, err, nil)
	assert.Equal(t, hasNeighbor(neighbors, badnode), false)
}

// test add route to routing table
func TestAddRoute1(t *testing.T) {
	t1, _ := tapestry.MakeTapestries(true, "1")
	t2, _ := tapestry.MakeTapestries(true, "22")
	defer tapestry.KillTapestries(t1[0], t2[0])

	err := t1[0].AddRoute(t2[0].Node)

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, err, nil)
	assert.Equal(t, hasRoutingTableNode(t1[0], t2[0].Node), true)
}

// test add route to routing table, replacing a node in routing table
func TestAddRoute2_ReplacingNode(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1", "341", "342")
	node343, tap, _ := tapestry.AddOne("343", tap[0].Node.Address, tap)
	defer tapestry.KillTapestries(tap...)

	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, hasRoutingTableNode(tap[0], tap[0].Node), true)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[1].Node), true)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[2].Node), true)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[3].Node), true)

	_, tap, _ = tapestry.AddOne("300", tap[0].Node.Address, tap)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(tap[0], node343.Node), false)
}

// test add route to routing table, not replacing a node in routing table, because new node is further
func TestAddRoute3_NotReplacingNode(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1", "341", "342")
	node343, tap, _ := tapestry.AddOne("343", tap[0].Node.Address, tap)
	defer tapestry.KillTapestries(tap...)

	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, hasRoutingTableNode(tap[0], tap[0].Node), true)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[1].Node), true)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[2].Node), true)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[3].Node), true)

	_, tap, _ = tapestry.AddOne("357", tap[0].Node.Address, tap)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(tap[0], node343.Node), true)
}

func hasNeighbor(slice []tapestry.RemoteNode, item tapestry.RemoteNode) bool {
	return hasnode(slice, item)
}

func hasnode(slice []tapestry.RemoteNode, item tapestry.RemoteNode) bool {
	set := make(map[tapestry.RemoteNode]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}

	_, ok := set[item]
	return ok
}
