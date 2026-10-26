// Copyright 2019 Dolthub, Inc.
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


// NOTICE (Apache License 2.0, section 4(b)): this file was added in 2026 by
// Brandon Fryslie for the links-issue-tracker (`lit`) project. It is not part
// of upstream github.com/dolthub/dolt. What it pins, and why, is recorded in
// the patch ledger that README.lit-fork.md at the root of this fork points to.

package dbfactory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/nbs"
)

// TestJournalLockFromParams pins that the params map is read into exactly one
// journal lock policy, and that naming two is refused rather than resolved.
func TestJournalLockFromParams(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string]interface{}
		want   nbs.JournalLock
	}{
		{"no params", nil, nbs.JournalLockOrReadOnly},
		{"neither", map[string]interface{}{ChunkJournalParam: struct{}{}}, nbs.JournalLockOrReadOnly},
		{"fail fast", map[string]interface{}{FailOnJournalLockTimeoutParam: struct{}{}}, nbs.JournalLockOrFail},
		{"read only", map[string]interface{}{JournalReadOnlyParam: struct{}{}}, nbs.JournalReadOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := journalLockFromParams(tc.params)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
	_, err := journalLockFromParams(map[string]interface{}{FailOnJournalLockTimeoutParam: struct{}{}, JournalReadOnlyParam: struct{}{}})
	assert.Error(t, err)
}
