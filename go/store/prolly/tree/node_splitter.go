// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// NOTICE (Apache License 2.0, section 4(b)): this file was modified in 2026 by
// Brandon Fryslie for the links-issue-tracker (`lit`) project. It is not the
// upstream github.com/dolthub/dolt version. What changed, why, and what would
// let the change be dropped are recorded in the patch ledger that
// README.lit-fork.md at the root of this fork points to.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package tree

import (
	"crypto/sha512"
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/zeebo/xxh3"
)

const (
	minChunkSize = 1 << 9
	maxChunkSize = 1 << 14
)

var levelSalt = [...]uint64{
	saltFromLevel(1),
	saltFromLevel(2),
	saltFromLevel(3),
	saltFromLevel(4),
	saltFromLevel(5),
	saltFromLevel(6),
	saltFromLevel(7),
	saltFromLevel(8),
	saltFromLevel(9),
	saltFromLevel(10),
	saltFromLevel(11),
	saltFromLevel(12),
	saltFromLevel(13),
	saltFromLevel(14),
	saltFromLevel(15),
}

// splitterFactory makes a nodeSplitter.
type splitterFactory func(level uint8) nodeSplitter

var defaultSplitterFactory splitterFactory = newKeySplitter

// nodeSplitter decides where Item streams should be split into chunks.
type nodeSplitter interface {
	// Append provides more nodeItems to the splitter. Splitter's make chunk
	// boundary decisions based on the Item contents. Upon return, callers
	// can use CrossedBoundary() to see if a chunk boundary has crossed.
	Append(key, values Item) error

	// CrossedBoundary returns true if the provided nodeItems have caused a chunk
	// boundary to be crossed.
	CrossedBoundary() bool

	// Reset resets the state of the splitter.
	Reset()
}

// keySplitter is a nodeSplitter that makes chunk boundary decisions on the hash of
// the key of an Item pair.
//
// keySplitter uses a dynamic threshold modeled on a weibull distribution
// (https://en.wikipedia.org/wiki/Weibull_distribution). As the size of the current
// trunk increases, it becomes easier to pass the threshold, reducing the likelihood
// of forming very large or very small chunks.
type keySplitter struct {
	count, size     uint32
	crossedBoundary bool

	salt uint64
}

func newKeySplitter(level uint8) nodeSplitter {
	return &keySplitter{
		salt: levelSalt[level],
	}
}

var _ splitterFactory = newKeySplitter

func (ks *keySplitter) Append(key, value Item) error {
	thisSize := uint32(len(key) + len(value))
	ks.size += thisSize

	if ks.size < minChunkSize {
		return nil
	}
	if ks.size > maxChunkSize {
		ks.crossedBoundary = true
		return nil
	}

	// TODO: is there a way to reduce weibullChecks?
	h := xxHash32(key, ks.salt)
	ks.crossedBoundary = weibullCheck(ks.size, thisSize, h)
	return nil
}

func (ks *keySplitter) CrossedBoundary() bool {
	return ks.crossedBoundary
}

func (ks *keySplitter) Reset() {
	ks.size = 0
	ks.crossedBoundary = false
}

const (
	targetSize float64 = 4096
	maxUint32  float64 = math.MaxUint32

	// weibull params
	K = 4.

	// TODO: seems like this should be targetSize / math.Gamma(1 + 1/K).
	L = targetSize
)

// weibullCheck returns true if we should split
// at |hash| for a given record inserted into a
// chunk of size |size|, where the record's size
// is |thisSize|. |size| is the size of the chunk
// after the record is inserted, so includes
// |thisSize| in it.
//
// weibullCheck attempts to form chunks whose
// sizes match the weibull distribution.
//
// The logic is as follows: given that we haven't
// split on any of the records up to |size - thisSize|,
// the probability that we should split on this record
// is (CDF(end) - CDF(start)) / (1 - CDF(start)), or,
// the percentage of the remaining portion of the CDF
// that this record actually covers. We split is |hash|,
// treated as a uniform random number between [0,1),
// is less than this percentage.
func weibullCheck(size, thisSize, hash uint32) bool {
	// Instead of using constant K = 4, we just manually multiply to avoid math.Pow call
	pow := float64(size-thisSize) / L
	start := -math.Expm1(-(pow * pow * pow * pow))

	pow = float64(size) / L
	end := -math.Expm1(-(pow * pow * pow * pow))

	p := float64(hash) / maxUint32
	d := 1 - start
	if d <= 0 {
		return true
	}
	target := (end - start) / d
	return p < target
}

func xxHash32(b []byte, salt uint64) uint32 {
	return uint32(xxh3.HashSeed(b, salt))
}

func saltFromLevel(level uint8) (salt uint64) {
	full := sha512.Sum512([]byte{level})
	return binary.LittleEndian.Uint64(full[:8])
}

// DeterministicHashLevel takes a key and counts the number of leading zeros in the key's hash.
// This is used for computing the level that a key appears in, in a ProximityMap
func DeterministicHashLevel(leadingZerosPerLevel uint8, key Item) uint8 {
	h := xxHash32(key, levelSalt[1])
	return uint8(bits.LeadingZeros32(h)) / leadingZerosPerLevel
}
