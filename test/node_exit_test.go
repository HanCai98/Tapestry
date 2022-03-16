package test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	tapestry "tapestry/pkg"
	"testing"
	"time"
)

// test node leaving in graceful way
func TestLeave1_SafeLeave(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1", "2")
	fmt.Printf("length of tap %d\n", len(tap))
	defer tapestry.KillTapestries(tap[0], tap[1])

	assert.Equal(t, hasRoutingTableNode(tap[0], tap[1].Node), true)

	err := tap[1].Leave()
	if err != nil {
		t.Errorf("error in notifyleave")
	}
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[1].Node), false)

}

// test node leaving in unsafe way
func TestLeave2_UnsafeLeave(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1", "2")
	fmt.Printf("length of tap %d\n", len(tap))
	defer tapestry.KillTapestries(tap[0], tap[1])

	assert.Equal(t, hasRoutingTableNode(tap[0], tap[1].Node), true)

	tap[1].Kill()

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[1].Node), true)

}

// test notify leaving without replacement node
func TestNotifyLeave1_WithoutReplacement(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1")
	fmt.Printf("length of tap %d\n", len(tap))
	defer tapestry.KillTapestries(tap[0])

	leaveNode := tapestry.RemoteNode{tapestry.MakeID("2"), "abcd"}
	tap[0].Table.Add(leaveNode)
	tap[0].Backpointers.Add(leaveNode)
	assert.Equal(t, hasRoutingTableNode(tap[0], leaveNode), true)

	err := tap[0].NotifyLeave(leaveNode, nil)
	if err != nil {
		t.Errorf("error in notifyleave")
	}
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(tap[0], leaveNode), false)
}

// test node NotifyLeave with replacement node
func TestNotifyLeave2_Replacement(t *testing.T) {
	t1, _ := tapestry.MakeTapestries(true, "1")
	t2, _ := tapestry.MakeTapestries(true, "22")

	defer tapestry.KillTapestries(t1[0], t2[0])

	leaveNode := tapestry.RemoteNode{tapestry.MakeID("2"), "abcd"}
	replacement := t2[0].Node

	t1[0].Table.Add(leaveNode)
	t1[0].Backpointers.Add(leaveNode)

	assert.Equal(t, hasRoutingTableNode(t1[0], leaveNode), true)
	assert.Equal(t, hasRoutingTableNode(t1[0], replacement), false)

	err := t1[0].NotifyLeave(leaveNode, &replacement)
	if err != nil {
		t.Errorf("error in notifyleave")
	}
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(t1[0], leaveNode), false)
	assert.Equal(t, hasRoutingTableNode(t1[0], replacement), true)

}

// test node leaving with replacement node
func TestNotifyLeave2_WithReplacement(t *testing.T) {
	t1, _ := tapestry.Start(tapestry.MakeID("114"), 0, "")
	t2, _ := tapestry.Start(tapestry.MakeID("214"), 0, t1.Node.Address)
	t3, _ := tapestry.Start(tapestry.MakeID("224"), 0, t2.Node.Address)
	t4, _ := tapestry.Start(tapestry.MakeID("234"), 0, t2.Node.Address)
	t5, _ := tapestry.Start(tapestry.MakeID("244"), 0, t2.Node.Address)
	defer tapestry.KillTapestries(t1, t2, t3, t4, t5)

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(t1, t5.Node), false)
	assert.Equal(t, hasRoutingTableNode(t2, t5.Node), true)
	assert.Equal(t, hasRoutingTableNode(t3, t5.Node), true)
	assert.Equal(t, hasRoutingTableNode(t4, t5.Node), true)

	t2.Leave()
	t3.Leave()
	t4.Leave()

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(t1, t5.Node), true)

}

// help function
func hasRoutingTableNode(node *tapestry.Node, node2 tapestry.RemoteNode) bool {
	return node.Table.Contains(node2)
}
