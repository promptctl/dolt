// Copyright 2026 Dolthub, Inc.
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

// NOTICE (Apache License 2.0, section 4(b)): this file is not part of upstream
// github.com/dolthub/dolt; it was added in 2026 by Brandon Fryslie for the
// links-issue-tracker (`lit`) project. What it pins, why, and what would let
// it be dropped are recorded in the patch ledger that README.lit-fork.md at
// the root of this fork points to.

package pull

import (
	"testing"
	"time"
)

// The stats consumer is torn down by its own context, which nothing orders
// relative to emitStats's done signal. cancel() must therefore return even
// when nobody is left draining the channel — a plain blocking send here is
// what used to pin Pull (and the caller's database engine) forever after a
// context cancellation.
func TestEmitStatsCancelReturnsWithoutConsumer(t *testing.T) {
	s := &stats{wrStatsGetter: func() PullTableFileWriterStats { return PullTableFileWriterStats{} }}
	ch := make(chan Stats) // unbuffered, and deliberately never read

	cancel := emitStats(s, ch)

	// Let the 1s update ticker fire so the sender is parked in a send with no
	// consumer — the worst-case shutdown state. If timing ever cuts this
	// short, the test still exercises the final flush path; it degrades,
	// never false-fails.
	time.Sleep(1500 * time.Millisecond)

	returned := make(chan struct{})
	go func() {
		cancel()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("emitStats cancel deadlocked: sender still blocked on the stats channel with no consumer")
	}
}
