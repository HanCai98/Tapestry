/*
 *  Brown University, CS138, Spring 2022
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package pkg

import (
	"sort"
	"sync"
)

// RoutingTable has a number of levels equal to the number of digits in an ID
// (default 40). Each level has a number of slots equal to the digit base
// (default 16). A node that exists on level n thereby shares a prefix of length
// n with the local node. Access to the routing table protected by a mutex.
type RoutingTable struct {
	local RemoteNode                 // The local tapestry node
	Rows  [DIGITS][BASE][]RemoteNode // The rows of the routing table
	mutex sync.Mutex                 // To manage concurrent access to the routing table (could also have a per-level mutex)
}

// NewRoutingTable creates and returns a new routing table, placing the local node at the
// appropriate slot in each level of the table.
func NewRoutingTable(me RemoteNode) *RoutingTable {
	t := new(RoutingTable)
	t.local = me

	// Create the node lists with capacity of SLOTSIZE
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			t.Rows[i][j] = make([]RemoteNode, 0, SLOTSIZE)
		}
	}

	// Make sure each row has at least our node in it
	for i := 0; i < DIGITS; i++ {
		slot := t.Rows[i][t.local.ID[i]]
		t.Rows[i][t.local.ID[i]] = append(slot, t.local)
	}

	return t
}

// Add adds the given node to the routing table.
//
// Note you should not add the node to preceding levels. You need to add the node
// to one specific slot in the routing table (or replace an element if the slot is full
// at SLOTSIZE).
//
// Returns true if the node did not previously exist in the table and was subsequently added.
// Returns the previous node in the table, if one was overwritten.
func (t *RoutingTable) Add(node RemoteNode) (added bool, previous *RemoteNode) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO: students should implement this
	if node.ID.String() == t.local.ID.String() {
		return false, nil
	}
	level := SharedPrefixLength(t.local.ID, node.ID)
	digit := node.ID[level]
	slot := &t.Rows[level][digit]
	// slot is not full
	if len(*slot) < SLOTSIZE {
		for i := 0; i < len(*slot); i++ {
			if (*slot)[i] == node {
				return false, nil
			}
		}
		*slot = append(*slot, node)
		sort.Slice(*slot, func(i int, j int) bool {
			return t.local.ID.Closer((*slot)[i].ID, (*slot)[j].ID)
		})
		return true, nil
	} else if len(*slot) == SLOTSIZE {
		// slot is full
		for i := 0; i < len(*slot); i++ {
			if (*slot)[i] == node {
				return false, nil
			}
		}
		// new node does not exist, remove the furthest node
		toRemove := node
		for i := 0; i < len(*slot); i++ {
			p := (*slot)[i]
			if t.local.ID.Closer(toRemove.ID, p.ID) {
				(*slot)[i] = toRemove
				toRemove = p
				added = true
			}
		}
		// sort the slot
		sort.Slice(*slot, func(i int, j int) bool {
			return t.local.ID.Closer((*slot)[i].ID, (*slot)[j].ID)
		})

		if toRemove != node {
			previous = &toRemove
			return added, previous
		}
	}

	return added, previous
}

// Remove removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
// Return false if a node tries to remove itself from the table.
func (t *RoutingTable) Remove(node RemoteNode) (wasRemoved bool) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO: students should implement this
	if node.ID.String() == t.local.ID.String() {
		return false
	}
	level := SharedPrefixLength(t.local.ID, node.ID)
	digit := node.ID[level]
	slot := &t.Rows[level][digit]
	size := len(*slot)
	for i := 0; i < size; i++ {
		if (*slot)[i] == node {
			*slot = append((*slot)[:i], (*slot)[i+1:]...)
			wasRemoved = true
			return wasRemoved
		}
	}
	return
}

// GetLevel gets ALL nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodes []RemoteNode) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO: students should implement this
	if level < 0 || level >= DIGITS {
		return nil
	}
	nodes = make([]RemoteNode, 0)
	for _, backup := range t.Rows[level] {
		for _, node := range backup {
			if node.ID.String() != t.local.ID.String() {
				nodes = append(nodes, node)
			}
		}
	}

	return
}

// FindClosestNode find the closest node in a slot compared with id
func FindClosestNode(id ID, slot []RemoteNode) *RemoteNode {
	result := &slot[0]
	for i, node := range slot {
		if id.Closer(node.ID, (*result).ID) {
			result = &slot[i]
		}
	}
	return result
}

// FindNextHop searches the table for the closest next-hop node for the provided ID starting at the given level.
func (t *RoutingTable) FindNextHop(id ID, level int32) RemoteNode {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO: students should implement this
	if level >= DIGITS || level < 0 {
		// exceed, just return the local node
		return t.local
	}

	for curLevel := level; curLevel < DIGITS; curLevel++ {
		col := id[curLevel]
		for i := 0; i < BASE; i++ {
			slot := t.Rows[curLevel][col]
			// we already have node in this slot
			if len(slot) != 0 {
				candidate := FindClosestNode(id, slot)
				if candidate.ID.String() != t.local.ID.String() {
					return *candidate
				} else {
					break
				}
			}

			col = (col + 1) % BASE
		}
	}
	return t.local
}
