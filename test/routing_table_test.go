package test

import (
	"github.com/stretchr/testify/assert"
	tapestry "tapestry/pkg"
	"testing"
	"time"
)

// test routing table Add function
func TestRoutingTable_Add(t *testing.T) {
	tap, _ := tapestry.MakeTapestries(true, "1", "2", "3")
	defer tapestry.KillTapestries(tap[0], tap[1], tap[2])

	assert.Equal(t, hasRoutingTableNode(tap[0], tap[1].Node), true)
	err := tap[1].Leave()
	if err != nil {
		t.Errorf("error in notifyleave")
	}
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, hasRoutingTableNode(tap[0], tap[1].Node), false)
}
