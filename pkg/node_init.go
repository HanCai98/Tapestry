/*
 *  Brown University, CS138, Spring 2022
 *
 *  Purpose: Defines global constants and functions to create and join a new
 *  node into a Tapestry mesh, and functions for altering the routing table
 *  and backpointers of the local node that are invoked over RPC.
 */

package pkg

import (
	"fmt"
	"net"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc"
)

// BASE is the base of a digit of an ID.  By default, a digit is base-16.
const BASE = 16

// DIGITS is the number of digits in an ID.  By default, an ID has 40 digits.
const DIGITS = 40

// RETRIES is the number of retries on failure. By default we have 3 retries.
const RETRIES = 3

// K is neigborset size during neighbor traversal before fetching backpointers. By default this has a value of 10.
const K = 10

// SLOTSIZE is the size each slot in the routing table should store this many nodes. By default this is 3.
const SLOTSIZE = 3

// REPUBLISH is object republish interval for nodes advertising objects.
const REPUBLISH = 10 * time.Second

// TIMEOUT is object timeout interval for nodes storing objects.
const TIMEOUT = 25 * time.Second

// Node is the main struct for the local Tapestry node. Methods can be invoked locally on this struct.
type Node struct {
	UnsafeTapestryRPCServer
	Node           RemoteNode    // The ID and address of this node
	Table          *RoutingTable // The routing table
	Backpointers   *Backpointers // Backpointers to keep track of other nodes that point to us
	LocationsByKey *LocationMap  // Stores keys for which this node is the root
	blobstore      *BlobStore    // Stores blobs on the local node
	server         *grpc.Server
}

func (local *Node) String() string {
	return fmt.Sprintf("Tapestry Node %v at %v", local.Node.ID, local.Node.Address)
}

// ID returns the tapestry node's ID in string format
func (local *Node) ID() string {
	return local.Node.ID.String()
}

// Addr returns the tapestry node's address in string format
func (local *Node) Addr() string {
	return local.Node.Address
}

// Called in tapestry initialization to create a tapestry node struct
func newTapestryNode(node RemoteNode) *Node {
	serverOptions := []grpc.ServerOption{}
	n := new(Node)

	n.Node = node
	n.Table = NewRoutingTable(node)
	n.Backpointers = NewBackpointers(node)
	n.LocationsByKey = NewLocationMap()
	n.blobstore = NewBlobStore()
	n.server = grpc.NewServer(serverOptions...)

	return n
}

// Start a node with the specified ID.
func Start(id ID, port int, connectTo string) (tapestry *Node, err error) {
	// Create the RPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		return nil, err
	}

	// Get the hostname of this machine
	name, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Unable to get hostname of local machine to start Tapestry node. Reason: %v", err)
	}

	// Get the port we are bound to
	_, actualport, err := net.SplitHostPort(lis.Addr().String()) //fmt.Sprintf("%v:%v", name, port)
	if err != nil {
		return nil, err
	}

	// The actual address of this node. NOTE: If gRPC calls fail with deadline exceeded errors, this could be that it
	// is unable to resolve the computer's hostname to the local IP address. Try uncommenting the below line if this
	// happens to you (please do not check this change into your Git repo).
	// name = "127.0.0.1"
	address := fmt.Sprintf("%s:%s", name, actualport)

	// Create the local node
	tapestry = newTapestryNode(RemoteNode{ID: id, Address: address})
	fmt.Printf("Created tapestry node %v\n", tapestry)
	Trace.Printf("Created tapestry node")

	RegisterTapestryRPCServer(tapestry.server, tapestry)
	fmt.Printf("Registered RPC Server\n")
	go tapestry.server.Serve(lis)

	// If specified, connect to the provided address
	if connectTo != "" {
		// Get the node we're joining
		node, err := SayHelloRPC(connectTo, tapestry.Node)
		if err != nil {
			return nil, fmt.Errorf("Error joining existing tapestry node %v, reason: %v", address, err)
		}
		err = tapestry.Join(node)
		if err != nil {
			return nil, err
		}
	}

	return tapestry, nil
}

// Join is invoked when starting the local node, if we are connecting to an existing Tapestry.
//
// - Find the root for our node's ID
// - Call AddNode on our root to initiate the multicast and receive our initial neighbor set. Add them to our table.
// - Iteratively get backpointers from the neighbor set for all levels in range [0, SharedPrefixLength]
// - and populate routing table
func (local *Node) Join(otherNode RemoteNode) (err error) {
	Debug.Println("Joining", otherNode)

	// Route to our root
	root, err := local.FindRootOnRemoteNode(otherNode, local.Node.ID)
	if err != nil {
		return fmt.Errorf("error joining existing tapestry node %v, reason: %v", otherNode, err)
	}
	// Add ourselves to our root by invoking AddNode on the remote node
	neighbors, err := root.AddNodeRPC(local.Node)
	if err != nil {
		return fmt.Errorf("error adding ourselves to root node %v, reason: %v", root, err)
	}

	// Add the neighbors to our local routing table.
	for _, n := range neighbors {
		local.AddRoute(n)
	}

	// TODO: students should implement the backpointer traversal portion of Join
	prefixLength := SharedPrefixLength(local.Node.ID, otherNode.ID)
	err = local.TraverseBackpointers(neighbors, prefixLength)
	if err != nil {
		return fmt.Errorf("error occurs during Join: %v", err)
	}
	return nil
}

func (local *Node) TraverseBackpointers(neighbors []RemoteNode, level int) (err error) {
	if level >= 0 {
		nextNeighbors := neighbors
		for _, neighbor := range neighbors {
			backpointers, err := neighbor.GetBackpointersRPC(local.Node, level)
			if err != nil {
				return fmt.Errorf("error occurs during traversal: %v", err)
			}
			// remove the duplicate nodes from backpointers
			// temp contains different nodes compared with nextNeighbors
			nextNeighbors = append(nextNeighbors, backpointers...)
			nextNeighbors = RemoveDuplicates(nextNeighbors)
		}

		for _, neighbor := range nextNeighbors {
			err = local.AddRoute(neighbor)
		}
		// sort the nextNeighbors and only take the first K nodes
		sort.SliceStable(nextNeighbors, func(i, j int) bool {
			return local.Node.ID.Closer(nextNeighbors[i].ID, nextNeighbors[j].ID)
		})
		// trimming down to K
		if len(nextNeighbors) > K {
			nextNeighbors = nextNeighbors[:K]
		}
		err = local.TraverseBackpointers(nextNeighbors, level-1)
	}
	return
}

// RemoveDuplicates Remove the duplicates from the input set
func RemoveDuplicates(input []RemoteNode) []RemoteNode {
	result := make([]RemoteNode, 0)
	temp := map[RemoteNode]struct{}{}
	for _, item := range input {
		if _, ok := temp[item]; !ok {
			temp[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

// AddNode adds node to the tapestry
//
// - Begin the acknowledged multicast
// - Return the neighborset from the multicast
func (local *Node) AddNode(node RemoteNode) (neighborset []RemoteNode, err error) {
	return local.AddNodeMulticast(node, SharedPrefixLength(node.ID, local.Node.ID))
}

// AddNodeMulticast sends newNode to need-to-know nodes participating in the multicast.
// - Perform multicast to need-to-know nodes
// - Add the route for the new node (use `local.addRoute`)
// - Transfer of appropriate replica info to the new node (use `local.locationsByKey.GetTransferRegistrations`)
//   If error, rollback the location map (add back unsuccessfully transferred objects)
//
// - Propagate the multicast to the specified row in our routing table and await multicast responses
// - Return the merged neighbor set
//
// - note: `local.table.GetLevel` does not return the local node so you must manually add this to the neighbors set

func (local *Node) AddNodeMulticast(newNode RemoteNode, level int) (neighbors []RemoteNode, err error) {
	Debug.Printf("Add node multicast %v at level %v\n", newNode, level)
	// TODO: students should implement this
	// root node contacts all nodes on levels ≥ n of its routing table
	neighbors = make([]RemoteNode, 0)
	if level < DIGITS {
		targets := local.Table.GetLevel(level)
		// Must include local node
		targets = append(targets, local.Node)
		results := make([]RemoteNode, 0)

		for _, target := range targets {
			// trigger a multicast to the next level of its routing table
			rsp, err := target.AddNodeMulticastRPC(newNode, level+1)
			if err != nil {
				local.RemoveBadNodes([]RemoteNode{target})
				//return nil, fmt.Errorf("error in multicast: %v\n", err)
				fmt.Printf("error in multicast: %v\n", err)
			}
			results = append(neighbors, rsp...)
		}

		results = append(results, targets...)
		local.AddRoute(newNode)
		newNode.TransferRPC(local.Node, local.LocationsByKey.GetTransferRegistrations(local.Node, newNode))

		neighbors = RemoveDuplicates(results)
	}

	return neighbors, err
}

func (local *Node) TransferRelevantObjects(newNode RemoteNode) {
	// get transfer data
	objects := local.LocationsByKey.GetTransferRegistrations(local.Node, newNode)
	if len(objects) > 0 {
		// transfer the data
		err := newNode.TransferRPC(local.Node, objects)
		if err != nil {
			// remove the new node, reinsert the transferred data
			local.RemoveBadNodes([]RemoteNode{newNode})
			local.LocationsByKey.RegisterAll(objects, TIMEOUT)
		}
	}

}

// AddBackpointer adds the from node to our backpointers, and possibly add the node to our
// routing table, if appropriate
func (local *Node) AddBackpointer(from RemoteNode) (err error) {
	if local.Backpointers.Add(from) {
		Debug.Printf("Added backpointer %v\n", from)
	}
	local.AddRoute(from)
	return
}

// RemoveBackpointer removes the from node from our backpointers
func (local *Node) RemoveBackpointer(from RemoteNode) (err error) {
	if local.Backpointers.Remove(from) {
		Debug.Printf("Removed backpointer %v\n", from)
	}
	return
}

// GetBackpointers gets all backpointers at the level specified, and possibly add the node to our
// routing table, if appropriate
func (local *Node) GetBackpointers(from RemoteNode, level int) (backpointers []RemoteNode, err error) {
	Debug.Printf("Sending level %v backpointers to %v\n", level, from)
	backpointers = local.Backpointers.Get(level)
	local.AddRoute(from)
	return
}

// RemoveBadNodes discards all the provided nodes
// - Remove each node from our routing table
// - Remove each node from our set of backpointers
func (local *Node) RemoveBadNodes(badnodes []RemoteNode) (err error) {
	for _, badnode := range badnodes {
		if local.Table.Remove(badnode) {
			Debug.Printf("Removed bad node %v\n", badnode)
		}
		if local.Backpointers.Remove(badnode) {
			Debug.Printf("Removed bad node backpointer %v\n", badnode)
		}
	}
	return
}

// AddRoute Utility function that adds a node to our routing table.
// - Adds the provided node to the routing table, if appropriate.
// - If the node was added to the routing table, notify the node of a backpointer
// - If an old node was removed from the routing table, notify the old node of a removed backpointer
func (local *Node) AddRoute(node RemoteNode) (err error) {
	// TODO: students should implement this
	added, removed := local.Table.Add(node)

	if added {
		err = node.AddBackpointerRPC(local.Node)
		if err != nil {
			return fmt.Errorf("error occurs during Add: %v", err)
		}
	}

	if removed != nil {
		err = removed.RemoveBackpointerRPC(local.Node)
		if err != nil {
			return fmt.Errorf("error occurs during Add: %v", err)
		}
	}

	return
}
