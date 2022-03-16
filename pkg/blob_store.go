/*
 *  Brown University, CS138, Spring 2022
 *
 *  Purpose: Defines BlobStore struct and provides get/put/delete methods for
 *  interacting with it.
 */

package pkg

import (
	"sync"
)

// BlobStore is a utility class tacked on to the tapestry DOLR.  You should not need
// to use this directly.
type BlobStore struct {
	blobs map[string]Blob
	sync.RWMutex
}

// Blob is an arbitrary collection of bytes
type Blob struct {
	bytes []byte
	done  chan bool
}

// NewBlobStore creates a new blobstore
func NewBlobStore() *BlobStore {
	bs := new(BlobStore)
	bs.blobs = make(map[string]Blob)
	return bs
}

// Get bytes from the blobstore
func (bs *BlobStore) Get(key string) ([]byte, bool) {
	blob, exists := bs.blobs[key]
	if exists {
		return blob.bytes, true
	}
	return nil, false
}

// Put bytes in the blobstore
func (bs *BlobStore) Put(key string, blob []byte, unregister chan bool) {
	bs.Lock()
	defer bs.Unlock()

	// If a previous blob exists, delete it
	previous, exists := bs.blobs[key]
	if exists {
		previous.done <- true
	}

	// Register the new one
	bs.blobs[key] = Blob{blob, unregister}
}

// Delete the blob and unregister it
func (bs *BlobStore) Delete(key string) bool {
	bs.Lock()
	defer bs.Unlock()

	// If a previous blob exists, unregister it
	previous, exists := bs.blobs[key]
	if exists {
		previous.done <- true
	}
	delete(bs.blobs, key)
	return exists
}

// DeleteAll removes all blobs from the BlobStore
func (bs *BlobStore) DeleteAll() {
	bs.Lock()
	defer bs.Unlock()

	for key, value := range bs.blobs {
		value.done <- true
		delete(bs.blobs, key)
	}
}
