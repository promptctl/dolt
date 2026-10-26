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

// NOTICE (Apache License 2.0, section 4(b)): this file was added in 2026 by
// Brandon Fryslie for the links-issue-tracker (`lit`) project. It is not part
// of upstream github.com/dolthub/dolt. What it pins, and why, is recorded in
// the patch ledger that README.lit-fork.md at the root of this fork points to.

package nbs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// captureLogrus sends logrus's output to a buffer for the rest of the test.
func captureLogrus(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := logrus.StandardLogger().Out
	logrus.SetOutput(&buf)
	t.Cleanup(func() { logrus.SetOutput(prev) })
	return &buf
}

// TestJournalWarningsCb pins that a store opened under an observer reports a
// warning to it and logs nothing, and that one opened without an observer logs
// it, as upstream does.
func TestJournalWarningsCb(t *testing.T) {
	warning := errors.New("invalid journal record length")

	t.Run("an observer receives the warning and nothing is logged", func(t *testing.T) {
		logged := captureLogrus(t)
		var observed []error
		ctx := WithJournalWarningObserver(context.Background(), func(err error) { observed = append(observed, err) })
		JournalWarningsCb(ctx)(warning)
		assert.Equal(t, []error{warning}, observed)
		assert.Empty(t, logged.String())
	})

	t.Run("without an observer the warning is logged", func(t *testing.T) {
		logged := captureLogrus(t)
		JournalWarningsCb(context.Background())(warning)
		assert.Contains(t, logged.String(), warning.Error())
	})
}

// TestALazyJournalBootstrapReportsToItsOwnCallback pins that a journal absent
// when its store opened, and bootstrapped at the first Open of it instead,
// reports through the callback the store was opened with, as an eager
// bootstrap does, and not through the logging default.
func TestALazyJournalBootstrapReportsToItsOwnCallback(t *testing.T) {
	cacheOnce.Do(makeGlobalCaches)
	logged := captureLogrus(t)
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Cleanup(func() { file.RemoveAll(dir) })

	m, err := newJournalManifest(ctx, dir, JournalLockOrReadOnly)
	require.NoError(t, err)
	p := newFSTablePersister(dir, NewUnlimitedMemQuotaProvider(), false)
	var observed []error
	j, err := newChunkJournal(ctx, types.Format_Default.VersionString(), dir, m, p.(*fsTablePersister), func(err error) { observed = append(observed, err) })
	require.NoError(t, err)
	t.Cleanup(func() { j.Close() })
	require.Nil(t, j.wr, "with no journal file, the store defers its bootstrap")

	path := filepath.Join(dir, chunkJournalName)
	wr := newTestJournalWriter(t, path)
	var root hash.Hash
	for h, cc := range randomCompressedChunks(4) {
		require.NoError(t, wr.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc))
		root = h
	}
	require.NoError(t, wr.commitRootHash(ctx, dherrors.FatalBehaviorError, root))
	require.NoError(t, wr.Close())
	appendGarbage(t, path)

	_, err = j.Open(ctx, journalAddr, 0, &Stats{})
	require.NoError(t, err)
	require.NotEmpty(t, observed, "the garbage tail is reported to the store's callback")
	assert.Empty(t, logged.String())
}
