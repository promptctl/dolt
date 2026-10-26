// Copyright 2022 Dolthub, Inc.
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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// fileState is every regular file in a directory, by name: its bytes and its
// modification time. Two equal states mean nothing wrote to the directory's
// files in between, including a write of the bytes they already held.
type fileState map[string]struct {
	data  []byte
	mtime time.Time
}

func readFileState(t *testing.T, dir string) fileState {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	state := fileState{}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		p := filepath.Join(dir, e.Name())
		info, err := os.Stat(p)
		require.NoError(t, err)
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		state[e.Name()] = struct {
			data  []byte
			mtime time.Time
		}{data, info.ModTime()}
	}
	return state
}

func requireSameFileState(t *testing.T, before, after fileState) {
	t.Helper()
	require.Equal(t, len(before), len(after), "files in the directory")
	for name, b := range before {
		a, ok := after[name]
		require.True(t, ok, "%s is gone", name)
		require.True(t, bytes.Equal(b.data, a.data), "%s bytes changed", name)
		require.True(t, b.mtime.Equal(a.mtime), "%s was written (mtime %s -> %s)", name, b.mtime, a.mtime)
	}
}

// waitPastMtimeGranularity sleeps long enough that a write after it cannot
// leave a file's mtime equal to one recorded before it.
func waitPastMtimeGranularity() { time.Sleep(20 * time.Millisecond) }

// appendGarbage appends a tail no record parser accepts (0xFF length bytes
// decode as an over-max record length): the shape an interrupted write leaves,
// which a writable bootstrap truncates.
func appendGarbage(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Write(bytes.Repeat([]byte{0xFF}, 8))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// TestReadOnlyJournalBootstrap pins that a read-only bootstrap serves what a
// writable one serves and writes neither file — over a journal with indexed
// batches, an unindexed tail whose lookups the index lacks, and a garbage tail,
// each of which a writable bootstrap repairs. The last step is its detector: a
// writable bootstrap of the same files changes them.
func TestReadOnlyJournalBootstrap(t *testing.T) {
	ctx := context.Background()
	path := newTestFilePath(t)
	idxPath := filepath.Join(filepath.Dir(path), journalIndexFileName)

	j := newTestJournalWriter(t, path)
	indexed := randomCompressedChunks(8)
	for _, cc := range indexed {
		require.NoError(t, j.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc))
	}
	var indexedRoot hash.Hash
	for h := range indexed {
		indexedRoot = h
		break
	}
	o := j.offset()
	require.NoError(t, j.commitRootHash(ctx, dherrors.FatalBehaviorError, indexedRoot))
	require.NoError(t, j.flushIndexRecord(ctx, indexedRoot, o))
	tail := randomCompressedChunks(8)
	for _, cc := range tail {
		require.NoError(t, j.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc))
	}
	var root hash.Hash
	for h := range tail {
		root = h
		break
	}
	require.NoError(t, j.commitRootHash(ctx, dherrors.FatalBehaviorError, root))
	require.NoError(t, j.Close())
	appendGarbage(t, path)

	readOnly := func(t *testing.T, warnings *[]error) {
		t.Helper()
		before := readFileState(t, filepath.Dir(path))
		waitPastMtimeGranularity()
		wr, ok, err := openReadOnlyJournal(ctx, path)
		require.NoError(t, err)
		require.True(t, ok)
		last, err := wr.bootstrapReadOnlyJournal(ctx, nil, func(err error) { *warnings = append(*warnings, err) })
		require.NoError(t, err)
		assert.Equal(t, root, last)
		for _, set := range []map[hash.Hash]CompressedChunk{indexed, tail} {
			for a, exp := range set {
				act, err := wr.getCompressedChunk(a)
				require.NoError(t, err)
				assert.Equal(t, exp, act)
			}
		}
		require.Nil(t, wr.indexWriter, "a read-only journal holds no index writer")
		require.NoError(t, wr.Close())
		requireSameFileState(t, before, readFileState(t, filepath.Dir(path)))
	}

	t.Run("reads every chunk and writes nothing", func(t *testing.T) {
		var warnings []error
		readOnly(t, &warnings)
	})

	t.Run("reads past a corrupt index, reports it, and writes nothing", func(t *testing.T) {
		pristine, err := os.ReadFile(idxPath)
		require.NoError(t, err)
		corruptJournalIndex(t, idxPath)
		t.Cleanup(func() { require.NoError(t, os.WriteFile(idxPath, pristine, 0666)) })
		var warnings []error
		readOnly(t, &warnings)
		reported := false
		for _, w := range warnings {
			reported = reported || errors.Is(w, errCorruptJournalIndex)
		}
		assert.True(t, reported, "the corrupt index is reported through warningsCb: %v", warnings)
	})

	t.Run("detector: a writable bootstrap of the same files repairs them", func(t *testing.T) {
		before := readFileState(t, filepath.Dir(path))
		waitPastMtimeGranularity()
		wr, ok, err := openJournalWriter(ctx, path)
		require.NoError(t, err)
		require.True(t, ok)
		last, err := wr.bootstrapJournal(ctx, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, root, last)
		require.NoError(t, wr.Close())
		after := readFileState(t, filepath.Dir(path))
		assert.Less(t, len(after[filepath.Base(path)].data), len(before[filepath.Base(path)].data), "the garbage tail is truncated")
		assert.False(t, before[journalIndexFileName].mtime.Equal(after[journalIndexFileName].mtime), "the index is rewound and its tail rewritten")
	})
}

// TestReadOnlyJournalNoIndex pins that a read-only bootstrap of a journal with
// no index serves it from the journal and does not create one.
func TestReadOnlyJournalNoIndex(t *testing.T) {
	ctx := context.Background()
	path := newTestFilePath(t)
	j := newTestJournalWriter(t, path)
	chnx := randomCompressedChunks(4)
	var root hash.Hash
	for h, cc := range chnx {
		require.NoError(t, j.writeCompressedChunk(ctx, dherrors.FatalBehaviorError, cc))
		root = h
	}
	require.NoError(t, j.commitRootHash(ctx, dherrors.FatalBehaviorError, root))
	require.NoError(t, j.Close())
	idxPath := filepath.Join(filepath.Dir(path), journalIndexFileName)
	require.NoError(t, os.Remove(idxPath))

	wr, ok, err := openReadOnlyJournal(ctx, path)
	require.NoError(t, err)
	require.True(t, ok)
	last, err := wr.bootstrapReadOnlyJournal(ctx, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, root, last)
	for a, exp := range chnx {
		act, err := wr.getCompressedChunk(a)
		require.NoError(t, err)
		assert.Equal(t, exp, act)
	}
	require.NoError(t, wr.Close())
	_, err = os.Stat(idxPath)
	assert.ErrorIs(t, err, os.ErrNotExist, "a read-only bootstrap creates no index")
}

func openTestJournalingStore(t *testing.T, dir string, lock JournalLock) *NomsBlockStore {
	t.Helper()
	cacheOnce.Do(makeGlobalCaches)
	st, err := NewLocalJournalingStoreWithOptions(context.Background(), types.Format_Default.VersionString(), dir, NewUnlimitedMemQuotaProvider(), false, nil, JournalingStoreOptions{Lock: lock})
	require.NoError(t, err)
	return st
}

// writeTestJournalingStore commits |n| chunks to a new journaling store in
// |dir| and closes it, returning the chunks; the last is the root.
func writeTestJournalingStore(t *testing.T, dir string, n int) []chunks.Chunk {
	t.Helper()
	ctx := context.Background()
	st := openTestJournalingStore(t, dir, JournalLockOrFail)
	var written []chunks.Chunk
	for i := 0; i < n; i++ {
		c := chunks.NewChunk([]byte{byte(i), byte(i >> 8), 'r', 'o'})
		require.NoError(t, st.Put(ctx, c, noopGetAddrs))
		written = append(written, c)
	}
	rt, err := st.Root(ctx)
	require.NoError(t, err)
	ok, err := st.Commit(ctx, written[n-1].Hash(), rt)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, st.Close())
	return written
}

// TestReadOnlyJournalingStoreWritesNothing pins the store-level contract a
// JournalReadOnly open makes, and that the fallback a held lock forces makes
// the same one: open, read, close, and every file in the directory is
// unchanged. Its detector is the default open with the lock free, which
// rewrites the manifest and the journal's bookkeeping.
func TestReadOnlyJournalingStoreWritesNothing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	written := writeTestJournalingStore(t, dir, 32)
	appendGarbage(t, filepath.Join(dir, chunkJournalName))

	readAll := func(t *testing.T, st *NomsBlockStore) {
		t.Helper()
		root, err := st.Root(ctx)
		require.NoError(t, err)
		assert.Equal(t, written[len(written)-1].Hash(), root)
		for _, c := range written {
			got, err := st.Get(ctx, c.Hash())
			require.NoError(t, err)
			assert.Equal(t, c.Data(), got.Data())
		}
	}
	check := func(t *testing.T, open func() *NomsBlockStore) {
		t.Helper()
		before := readFileState(t, dir)
		waitPastMtimeGranularity()
		st := open()
		assert.Equal(t, chunks.ExclusiveAccessMode(chunks.ExclusiveAccessMode_ReadOnly), st.AccessMode())
		readAll(t, st)
		require.NoError(t, st.Close())
		requireSameFileState(t, before, readFileState(t, dir))
	}

	t.Run("JournalReadOnly with the lock free", func(t *testing.T) {
		check(t, func() *NomsBlockStore { return openTestJournalingStore(t, dir, JournalReadOnly) })
	})
	t.Run("the fallback under a held lock", func(t *testing.T) {
		w := openTestJournalingStore(t, dir, JournalLockOrFail)
		defer func() { require.NoError(t, w.Close()) }()
		check(t, func() *NomsBlockStore { return openTestJournalingStore(t, dir, JournalLockOrReadOnly) })
	})
	t.Run("JournalReadOnly with no index", func(t *testing.T) {
		require.NoError(t, os.Remove(filepath.Join(dir, journalIndexFileName)))
		check(t, func() *NomsBlockStore { return openTestJournalingStore(t, dir, JournalReadOnly) })
		_, err := os.Stat(filepath.Join(dir, journalIndexFileName))
		assert.ErrorIs(t, err, os.ErrNotExist)
	})
	t.Run("detector: the default open with the lock free writes", func(t *testing.T) {
		before := readFileState(t, dir)
		waitPastMtimeGranularity()
		st := openTestJournalingStore(t, dir, JournalLockOrReadOnly)
		readAll(t, st)
		require.NoError(t, st.Close())
		after := readFileState(t, dir)
		assert.False(t, before[manifestFileName].mtime.Equal(after[manifestFileName].mtime), "the manifest is rewritten")
	})
}

// TestReadOnlyJournalingStoreHoldsNoLock pins that a JournalReadOnly store
// keeps no writer out: a fail-fast open succeeds while it is open, and the
// reader goes on serving the root it opened on.
func TestReadOnlyJournalingStoreHoldsNoLock(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	written := writeTestJournalingStore(t, dir, 4)

	reader := openTestJournalingStore(t, dir, JournalReadOnly)
	defer func() { require.NoError(t, reader.Close()) }()
	writer := openTestJournalingStore(t, dir, JournalLockOrFail)
	assert.Equal(t, chunks.ExclusiveAccessMode(chunks.ExclusiveAccessMode_Exclusive), writer.AccessMode())
	require.NoError(t, writer.Close())

	root, err := reader.Root(ctx)
	require.NoError(t, err)
	assert.Equal(t, written[len(written)-1].Hash(), root)
}
