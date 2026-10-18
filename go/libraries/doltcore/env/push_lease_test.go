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

// NOTICE: this file does not exist upstream. It was added to this fork in 2026
// by Brandon Fryslie for the links-issue-tracker (`lit`) project as the
// regression test for patch 9; see the patch ledger README.lit-fork.md at the
// root of this fork points to.

package env

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestApplyPushLease(t *testing.T) {
	const commit = "0123456789abcdefghijklmnopqrstuv"
	expected := hash.Parse(commit)
	targets := func() []*PushTarget {
		return []*PushTarget{
			{SrcRef: ref.NewBranchRef("main"), DestRef: ref.NewBranchRef("main")},
			{SrcRef: ref.NewBranchRef("scratch"), DestRef: ref.NewBranchRef("other")},
		}
	}

	t.Run("leases exactly the named destination", func(t *testing.T) {
		ts := targets()
		require.NoError(t, ApplyPushLease(ts, "other:"+commit))
		assert.Equal(t, ref.UpdateMode{Force: true, Lease: &expected}, ts[1].Mode)
		assert.Equal(t, ref.UpdateMode{}, ts[0].Mode)
	})

	for _, lease := range []string{
		"main",                      // no expected commit
		"main:",                     // empty expected commit
		":" + commit,                // no branch
		"main:" + commit[:31],       // not a whole hash
		"main:" + commit + "0",      // not a whole hash
		"main:not-a-commit-hash",    // not a hash
		"scratch:" + commit,         // a source, not a destination
		"refs/heads/main:" + commit, // the destination is named as the push writes it
	} {
		t.Run("refuses "+lease, func(t *testing.T) {
			ts := targets()
			assert.Error(t, ApplyPushLease(ts, lease))
			for _, target := range ts {
				assert.Equal(t, ref.UpdateMode{}, target.Mode)
			}
		})
	}
}
