/*
 *  Brown University, CS138, Spring 2022
 *
 *  Purpose: Allows third-party clients to connect to a Tapestry node (such as
 *  a web app, mobile app, or CLI that you write), and put and get objects.
 */

package pkg

import "fmt"

// Client connects to a tapestry node
type Client struct {
	ID   string
	node *RemoteNode
}

// Connect to a Tapestry node
func Connect(addr string) (*Client, error) {
	node, err := SayHelloRPC(addr, RemoteNode{})
	if err != nil {
		Error.Printf("Failed to make connection to Tapestry node\n")
		return nil, err
	}
	return &Client{node.ID.String(), &node}, nil
}

// Store invokes tapestry.Store on the remote Tapestry node
func (client *Client) Store(key string, value []byte) error {
	Debug.Printf("Making remote TapestryStore call\n")
	return client.node.TapestryStoreRPC(key, value)
}

// Lookup invokes tapestry.Lookup on a remote Tapestry node
func (client *Client) Lookup(key string) ([]*Client, error) {
	Debug.Printf("Making remote TapestryLookup call\n")
	nodes, err := client.node.TapestryLookupRPC(key)
	clients := make([]*Client, len(nodes))
	for i, n := range nodes {
		clients[i] = &Client{n.ID.String(), &n}
	}
	return clients, err
}

// Get data from a Tapestry node. Looks up key then fetches directly.
func (client *Client) Get(key string) ([]byte, error) {
	Debug.Printf("Making remote TapestryGet call\n")
	// Lookup the key
	replicas, err := client.node.TapestryLookupRPC(key)
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
