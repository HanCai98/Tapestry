/*
 *  Brown University, CS138, Spring 2022
 *
 *  Purpose: Defines IDs for tapestry and provides various utility functions
 *  for manipulating and creating them. Provides functions to compare IDs
 *  for insertion into routing tables, and for implementing the routing
 *  algorithm.
 */

package pkg

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"time"
)

// An ID is a digit array
type ID [DIGITS]Digit

// Digit is just a typedef'ed uint8
type Digit uint8

// Random number generator for generating random node ID
var random = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

// RandomID returns a random ID.
func RandomID() ID {
	var id ID
	for i := range id {
		id[i] = Digit(random.Intn(BASE))
	}
	return id
}

// Hash hashes the string to an ID
func Hash(key string) (id ID) {
	// Sha-hash the key
	sha := sha1.New()
	sha.Write([]byte(key))
	hash := sha.Sum([]byte{})

	// Store in an ID
	for i := range id {
		id[i] = Digit(hash[(i/2)%len(hash)])
		if i%2 == 0 {
			id[i] >>= 4
		}
		id[i] %= BASE
	}

	return id
}

// SharedPrefixLength returns the length of the prefix that is shared by the two IDs.
func SharedPrefixLength(a ID, b ID) (i int) {
	// TODO: students should implement this function
	i = 0
	for index := 0; index < DIGITS; index++ {
		if a[index] == b[index] {
			i++
		} else {
			break
		}
	}
	return i
}

// IsNewRoute Used by Tapestry's surrogate routing.  Given IDs newId and currentId, will id now route
// to newId?
//
// In our surrogate routing, we move right from the missing cell until we find a non-missing cell
// with a node.
//
// 	The better choice in routing between newId and currentId is the id that:
//  - has the longest shared prefix with id
//  - if both have prefix of length n, which input has a better (n+1)th digit?
//  - if both have the same (n+1)th digit, consider (n+2)th digit, etc.
//
// We define a "better" digit as the closer digit when moving right from id.
// HINT: We can use MOD for this comparison.
//
// IsNewRoute returns true if newId is the better choice.
// Returns false if currentId is the better choice or if newId == currentId.
func (id ID) IsNewRoute(newId ID, currentId ID) bool {
	// TODO: students should implement this
	if newId == currentId {
		return false
	}

	prefixNewId := SharedPrefixLength(id, newId)
	prefixCurrentId := SharedPrefixLength(id, currentId)

	if prefixNewId == prefixCurrentId {
		index := prefixNewId
		for newId[index] == currentId[index] {
			index++
		}
		newDistance := math.Abs(float64(newId[index] - id[index]))
		curDistance := math.Abs(float64(currentId[index] - id[index]))

		if newDistance > curDistance {
			return false
		} else if newDistance < curDistance {
			return true
		}

	}

	return prefixNewId > prefixCurrentId
}

// Closer is used when inserting nodes into Tapestry's routing table.  If the routing
// table has multiple candidate nodes for a slot, then it chooses the node that
// is closer to the local node.
//
// In a production Tapestry implementation, closeness is determined by looking
// at the round-trip-times (RTTs) between (a, id) and (b, id); the node with the
// shorter RTT is closer.
//
// In this implementation, we have decided to define closeness as the absolute
// value of the difference between a and b. This is NOT the same as your
// implementation of BetterChoice.
//
// Return true if a is closer than b.
// Return false if b is closer than a, or if a == b.
func (id ID) Closer(first ID, second ID) bool {
	// TODO: students should implement this
	target := id.Big()
	firstDistance := big.NewInt(0).Abs(big.NewInt(0).Sub(target, first.Big()))
	secondDistance := big.NewInt(0).Abs(big.NewInt(0).Sub(target, second.Big()))
	result := firstDistance.Cmp(secondDistance)
	return result == -1
}

// Big Helper function: convert an ID to a big int.
func (id ID) Big() (b *big.Int) {
	b = big.NewInt(0)
	base := big.NewInt(BASE)
	for _, digit := range id {
		b.Mul(b, base)
		b.Add(b, big.NewInt(int64(digit)))
	}
	return b
}

// String representation of an ID is hexstring of each digit.
func (id ID) String() string {
	var buf bytes.Buffer
	for _, d := range id {
		buf.WriteString(d.String())
	}
	return buf.String()
}

// String representation of a digit is its hex value
func (digit Digit) String() string {
	return fmt.Sprintf("%X", byte(digit))
}

func (id ID) bytes() []byte {
	b := make([]byte, len(id))
	for idx, d := range id {
		b[idx] = byte(d)
	}
	return b
}

func idFromBytes(b []byte) (i ID) {
	if len(b) < DIGITS {
		return
	}
	for idx, d := range b[:DIGITS] {
		i[idx] = Digit(d)
	}
	return
}

// ParseID parses an ID from String
func ParseID(stringID string) (ID, error) {
	var id ID

	if len(stringID) != DIGITS {
		return id, fmt.Errorf("Cannot parse %v as ID, requires length %v, actual length %v", stringID, DIGITS, len(stringID))
	}

	for i := 0; i < DIGITS; i++ {
		d, err := strconv.ParseInt(stringID[i:i+1], 16, 0)
		if err != nil {
			return id, err
		}
		id[i] = Digit(d)
	}

	return id, nil
}
