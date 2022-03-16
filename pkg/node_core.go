/*
 *  Brown University, CS138, Spring 2022
 *
 *  Purpose: Defines functions to publish and lookup objects in a Tapestry mesh
 */

package pkg

import (
	"fmt"
	"time"
)

// Store a blob on the local node and publish the key to the tapestry.
func (local *Node) Store(key string, value []byte) (err error) {
	done, err := local.Publish(key)
	if err != nil {
		return err
	}
	local.blobstore.Put(key, value, done)
	return nil
}

// Get looks up a key in the tapestry then fetch the corresponding blob from the
// remote blob store.
func (local *Node) Get(key string) ([]byte, error) {
	// Lookup the key
	replicas, err := local.Lookup(key)
	if err != nil {
		return nil, err
	}
	if len(replicas) == 0 {
		return nil, fmt.Errorf("No replicas returned for key %v", key)
	}

	// Contact replicas
	var errs []error
	for _, replica := range replicas {
		blob, err := replica.BlobStoreFetchRPC(key)
		if err != nil {
			errs = append(errs, err)
		}
		if blob != nil {
			return *blob, nil
		}
	}

	return nil, fmt.Errorf("Error contacting replicas, %v: %v", replicas, errs)
}

// Remove the blob from the local blob store and stop advertising
func (local *Node) Remove(key string) bool {
	return local.blobstore.Delete(key)
}

// Publish Publishes the key in tapestry.
//
// - Start periodically publishing the key. At each publishing:
// 		- Find the root node for the key
// 		- Register the local node on the root
// 		- if anything failed, retry; until RETRIES has been reached.
// - Return a channel for cancelling the publish
// 		- if receiving from the channel, stop republishing
//
// Some note about publishing behavior:
// - The first publishing attempt should attempt to retry at most RETRIES times if there is a failure.
//   i.e. if RETRIES = 3 and FindRoot errored or returned false after all 3 times, consider this publishing
//   attempt as failed. The error returned for Publish should be the error message associated with the final
//   retry.
// - If any of these attempts succeed, you do not need to retry.
// - In addition to the initial publishing attempt, you should repeat this entire publishing workflow at the
//   appropriate interval. i.e. every 5 seconds we attempt to publish, and THIS publishing attempt can either
//  succeed, or fail after at most RETRIES times.
// - Keep trying to republish regardless of how the last attempt went
func (local *Node) Publish(key string) (cancel chan bool, err error) {
	// TODO: students should implement this
	err = local.AttemptPublish(key)
	if err != nil {
		return
	}
	cancel = make(chan bool)
	go func() {
		ticker := time.NewTimer(REPUBLISH)
		for i := 0; i < RETRIES; i++ {
			select {
			case <-ticker.C:
				err = local.AttemptPublish(key)
				if err != nil {
					return
				}
			case <-cancel:
				return
			}
		}
	}()

	return
}

func (local *Node) AttemptPublish(key string) (err error) {
	counter := 0
	for counter < RETRIES {
		root, err := local.FindRootOnRemoteNode(local.Node, Hash(key))
		if err != nil {
			counter++
			continue
		}
		isRoot, err := root.RegisterRPC(key, local.Node)
		if err != nil {
			local.RemoveBadNodes([]RemoteNode{root})
			counter++
		} else if !isRoot {
			counter++
		} else {
			return nil
		}
	}
	return fmt.Errorf("publish %v after %v failures", key, counter)
}

// Lookup look up the Tapestry nodes that are storing the blob for the specified key.
//
// - Find the root node for the key
// - Fetch the replicas (nodes storing the blob) from the root's location map
// - Attempt up to RETRIES times
func (local *Node) Lookup(key string) (nodes []RemoteNode, err error) {
	// TODO: students should implement this
	root, err := local.FindRootOnRemoteNode(local.Node, Hash(key))
	if err != nil {
		return nodes, fmt.Errorf("find error in Lookup: %v", err)
	}
	done, replicas, err := root.FetchRPC(key)
	if err != nil {
		for i := 0; !done && i < RETRIES-1; i++ {
			done, replicas, err = root.FetchRPC(key)
		}
	}
	return replicas, err
}

// FindRoot returns the root for id by recursive RPC calls on the next hop found in our routing table
// 		- find the next hop from our routing table
// 		- call FindRoot on nextHop
// 		- if failed, add nextHop to toRemove, remove them from local routing table, retry
func (local *Node) FindRoot(id ID, level int32) (root RemoteNode, toRemove *NodeSet, err error) {
	// TODO: students should implement this
	toRemove = NewNodeSet()
	for {
		if level >= DIGITS {
			return local.Table.local, toRemove, nil
		}
		// search level
		node := local.Table.FindNextHop(id, level)

		if node == local.Table.local {
			level += 1
			continue
		}

		root, addToRemove, err := node.FindRootRPC(id, level+1)
		if err != nil {
			toRemove.Add(node)
			local.Table.Remove(node)
			continue
		}
		toRemove.AddAll(addToRemove.Nodes())
		local.RemoveBadNodes(toRemove.Nodes())
		return root, toRemove, nil
	}
}

// Register The replica that stores some data with key is registering themselves to us as an advertiser of the key.
// - Check that we are the root node for the key, set `isRoot`
// - Add the node to the location map (local.locationsByKey.Register)
// 		- local.locationsByKey.Register kicks off a timer to remove the node if it's not advertised again
// 		  after TIMEOUT
func (local *Node) Register(key string, replica RemoteNode) (isRoot bool) {
	// TODO: students should implement this
	root, _, err := local.FindRoot(Hash(key), 0)
	if err != nil {
		fmt.Printf("Find error in Register: %v\n", err)
		return isRoot
	}
	if root == local.Node {
		isRoot = true
		local.LocationsByKey.Register(key, replica, TIMEOUT)
	}
	return isRoot
}

// Fetch checks that we are the root node for the requested key and
// return all nodes that are registered in the local location map for this key
func (local *Node) Fetch(key string) (isRoot bool, replicas []RemoteNode) {
	// TODO: students should implement this
	root, _, err := local.FindRoot(Hash(key), 0)
	if err != nil {
		fmt.Printf("Find error in Register: %v\n", err)
		return isRoot, replicas
	}
	if root == local.Node {
		isRoot = true
		replicas = local.LocationsByKey.Get(key)
	}
	return isRoot, replicas

}

// Transfer registers all of the provided objects in the local location map. (local.locationsByKey.RegisterAll)
// If appropriate, add the from node to our local routing table
func (local *Node) Transfer(from RemoteNode, replicaMap map[string][]RemoteNode) (err error) {
	// TODO: students should implement this
	if len(replicaMap) > 0 {
		local.LocationsByKey.RegisterAll(replicaMap, TIMEOUT)
	}
	err = local.AddRoute(from)
	return err
}

// FindRootOnRemoteNode calls FindRoot on a remote node with given ID
func (local *Node) FindRootOnRemoteNode(start RemoteNode, id ID) (RemoteNode, error) {
	// TODO: students should implement this
	root, _, err := start.FindRootRPC(id, 0)
	if err != nil {
		return RemoteNode{}, err
	}
	return root, err
}
