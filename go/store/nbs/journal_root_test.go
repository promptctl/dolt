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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// commitTestChunk commits one new chunk to an open store as its root.
func commitTestChunk(t *testing.T, st *NomsBlockStore, seed byte) chunks.Chunk {
	t.Helper()
	ctx := context.Background()
	c := chunks.NewChunk([]byte{seed, 'r', 'o', 'o', 't'})
	require.NoError(t, st.Put(ctx, c, noopGetAddrs))
	last, err := st.Root(ctx)
	require.NoError(t, err)
	ok, err := st.Commit(ctx, c.Hash(), last)
	require.NoError(t, err)
	require.True(t, ok)
	return c
}

func requireRoot(t *testing.T, st *NomsBlockStore, want hash.Hash) {
	t.Helper()
	root, err := st.Root(context.Background())
	require.NoError(t, err)
	assert.Equal(t, want, root)
}

// TestJournalHoldsARootItsManifestDoesNot pins that a root commit reaches the
// journal alone. The backing manifest keeps the root of its last table-set
// write, and every open reads the journal's root over it.
func TestJournalHoldsARootItsManifestDoesNot(t *testing.T) {
	dir := t.TempDir()
	writeTestJournalingStore(t, dir, 4)
	before := readFileState(t, dir)

	st := openTestJournalingStore(t, dir, JournalLockOrFail)
	waitPastMtimeGranularity()
	var latest chunks.Chunk
	for seed := byte(0); seed < 3; seed++ {
		latest = commitTestChunk(t, st, seed)
	}
	require.NoError(t, st.Close())

	after := readFileState(t, dir)
	assert.Equal(t, before[manifestFileName].data, after[manifestFileName].data, "manifest bytes")
	assert.True(t, before[manifestFileName].mtime.Equal(after[manifestFileName].mtime), "the manifest was written")

	for _, tc := range []struct {
		name string
		lock JournalLock
	}{
		{"a writable open", JournalLockOrFail},
		{"a read-only open", JournalReadOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestJournalingStore(t, dir, tc.lock)
			defer func() { require.NoError(t, st.Close()) }()
			requireRoot(t, st, latest.Hash())
			got, err := st.Get(context.Background(), latest.Hash())
			require.NoError(t, err)
			assert.Equal(t, latest.Data(), got.Data())
		})
	}
}

// TestWritableOpenOfACleanJournalLeavesItAlone pins that an open with no tail to
// repair and no table set to record writes neither the journal nor the
// manifest, where a journal with a tail to cut is repaired.
func TestWritableOpenOfACleanJournalLeavesItAlone(t *testing.T) {
	dir := t.TempDir()
	written := writeTestJournalingStore(t, dir, 8)
	openAndClose := func(t *testing.T) {
		t.Helper()
		st := openTestJournalingStore(t, dir, JournalLockOrFail)
		requireRoot(t, st, written[len(written)-1].Hash())
		require.NoError(t, st.Close())
	}

	before := readFileState(t, dir)
	waitPastMtimeGranularity()
	openAndClose(t)
	after := readFileState(t, dir)
	for _, name := range []string{chunkJournalName, manifestFileName} {
		assert.Equal(t, before[name].data, after[name].data, "%s bytes", name)
		assert.True(t, before[name].mtime.Equal(after[name].mtime), "%s was written", name)
	}

	appendGarbage(t, filepath.Join(dir, chunkJournalName))
	withTail := readFileState(t, dir)
	waitPastMtimeGranularity()
	openAndClose(t)
	repaired := readFileState(t, dir)
	assert.Less(t, len(repaired[chunkJournalName].data), len(withTail[chunkJournalName].data), "the tail was cut")
}

// TestCloseRecordsATableSetItsManifestDoesNotHold pins the one write Close keeps:
// a manifest that is missing, or that names another table set, takes the
// journal's, because the journal cannot rebuild a table set the way it answers
// for a root.
func TestCloseRecordsATableSetItsManifestDoesNotHold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		split func(t *testing.T, dir string, j *ChunkJournal)
	}{
		{"a manifest that is missing", func(t *testing.T, dir string, _ *ChunkJournal) {
			require.NoError(t, os.Remove(filepath.Join(dir, manifestFileName)))
		}},
		{"a manifest naming another table set", func(_ *testing.T, _ string, j *ChunkJournal) {
			j.contents.specs = append(j.contents.specs, tableSpec{name: hash.Of([]byte("rolled back")), chunkCount: 1})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeTestJournalingStore(t, dir, 4)
			st := openTestJournalingStore(t, dir, JournalLockOrFail)
			c := commitTestChunk(t, st, 9)
			j := st.persister.(*ChunkJournal)
			tc.split(t, dir, j)
			specs := j.contents.specs
			require.NoError(t, st.Close())

			f, err := os.Open(filepath.Join(dir, manifestFileName))
			require.NoError(t, err)
			defer f.Close()
			onDisk, err := parseManifest(f)
			require.NoError(t, err)
			assert.True(t, equalSpecs(specs, onDisk.specs), "the manifest holds %v, want %v", onDisk.specs, specs)
			assert.Equal(t, c.Hash(), onDisk.root)
		})
	}
}

// TestRootlessJournalAgreesWithItsManifest pins the open of a journal holding no
// root record, which a process that died before its first root leaves. The
// manifest takes the journal's empty root, so the block store and the journal
// give one answer, and a commit lands and survives a reopen.
func TestRootlessJournalAgreesWithItsManifest(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		specs func(mc manifestContents) []tableSpec
	}{
		{"a manifest that names the journal", func(mc manifestContents) []tableSpec { return mc.specs }},
		{"a manifest that does not", func(manifestContents) []tableSpec { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeTestJournalingStore(t, dir, 4)

			jm, err := newJournalManifest(ctx, dir, JournalLockOrFail)
			require.NoError(t, err)
			ok, mc, err := jm.ParseIfExists(ctx, &Stats{}, nil)
			require.NoError(t, err)
			require.True(t, ok)
			next := mc
			next.specs = tc.specs(mc)
			next.lock = generateLockHash(next.root, next.specs, next.appendix, nil)
			_, err = jm.Update(ctx, dherrors.FatalBehaviorError, mc.lock, next, &Stats{}, nil)
			require.NoError(t, err)
			require.NoError(t, jm.Close())
			require.NoError(t, os.Truncate(filepath.Join(dir, chunkJournalName), 0))
			require.NoError(t, os.Remove(filepath.Join(dir, journalIndexFileName)))

			st := openTestJournalingStore(t, dir, JournalLockOrFail)
			requireRoot(t, st, hash.Hash{})
			f, err := os.Open(filepath.Join(dir, manifestFileName))
			require.NoError(t, err)
			defer f.Close()
			onDisk, err := parseManifest(f)
			require.NoError(t, err)
			assert.Equal(t, hash.Hash{}, onDisk.root)
			c := commitTestChunk(t, st, 9)
			require.NoError(t, st.Close())

			st = openTestJournalingStore(t, dir, JournalLockOrFail)
			defer func() { require.NoError(t, st.Close()) }()
			requireRoot(t, st, c.Hash())
		})
	}
}
