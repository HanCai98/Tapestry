/*
 *  Brown University, CS138, Spring 2022
 *
 *  Purpose: Defines functions for a node leaving the Tapestry mesh, and
 *  transferring its stored locations to a new node.
 */

package pkg

// Kill this node without gracefully leaving the tapestry.
func (local *Node) Kill() {
	local.blobstore.DeleteAll()
	local.server.Stop()
}

// Leave gracefully exits the Tapestry mesh.
//
// - Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
// - If possible, give each backpointer a suitable alternative node from our routing table
func (local *Node) Leave() (err error) {
	// TODO: students should implement this
	var replacement *RemoteNode
	for i := DIGITS - 1; i >= 0; i-- {
		backpointers := local.Backpointers.Get(i)
		// notify backpointers
		for _, node := range backpointers {

			err := node.NotifyLeaveRPC(local.Node, replacement)
			if err != nil {
				local.RemoveBadNodes([]RemoteNode{node})
			}
		}
		// find replacement
		levels := local.Table.GetLevel(i)
		if len(levels) > 0 {
			replacement = &levels[0]
		} else {
			replacement = nil
		}
	}

	local.blobstore.DeleteAll()
	go local.server.GracefulStop()
	return err
}

// NotifyLeave occurs when another node is informing us of a graceful exit.
// - Remove references to the `from` node from our routing table and backpointers
// - If replacement is not nil or `RemoteNode{}`, add replacement to our routing table
func (local *Node) NotifyLeave(from RemoteNode, replacement *RemoteNode) (err error) {
	Debug.Printf("Received leave notification from %v with replacement node %v\n", from, replacement)

	// TODO: students should implement this
	local.Table.Remove(from)
	local.Backpointers.Remove(from)
	empty := RemoteNode{}
	if replacement != nil && *replacement != empty {
		err = local.AddRoute(*replacement)
	}

	return
}
