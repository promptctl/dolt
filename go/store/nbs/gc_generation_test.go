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
// regression test for patch 10; see the patch ledger README.lit-fork.md at the
// root of this fork points to.

package nbs

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/blobstore"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

// refsTo reads a test chunk's references off its own bytes: a chunk written
// as "->" followed by an address references the chunk at that address.
func refsTo(c chunks.Chunk) chunks.GetAddrsCb {
	return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error {
		if target, ok := strings.CutPrefix(string(c.Data()), "->"); ok {
			addrs.Insert(hash.Parse(target))
		}
		return nil
	}
}

func referencing(target chunks.Chunk) chunks.Chunk {
	return chunks.NewChunk([]byte("->" + target.Hash().String()))
}

// writeTableOf writes one table file holding cs to st, without adding it to
// st's manifest, and returns the ids AddTableFilesToManifest takes.
func writeTableOf(t *testing.T, ctx context.Context, st *NomsBlockStore, cs ...chunks.Chunk) map[string]int {
	t.Helper()
	var data [][]byte
	for _, c := range cs {
		data = append(data, c.Data())
	}
	table, addr, err := buildTable(data)
	require.NoError(t, err)
	require.NoError(t, st.WriteTableFile(ctx, addr.String(), 0, len(cs), nil, func() (io.ReadCloser, uint64, error) {
		return io.NopCloser(bytes.NewReader(table)), uint64(len(table)), nil
	}))
	return map[string]int{addr.String(): len(cs)}
}

// TestAWriterThatFindsANewGCGenerationReChecksAgainstIt is a writer whose view
// of a shared store predates another process replacing the store's table set
// under a new GC generation. Adding a table file whose chunk references
// one the replacement dropped fails loudly, and one whose references the new
// generation holds lands on it; neither waits on the old view.
func TestAWriterThatFindsANewGCGenerationReChecksAgainstIt(t *testing.T) {
	ctx := context.Background()
	bs := blobstore.NewLocalBlobstore(t.TempDir())
	open := func() *NomsBlockStore {
		st, err := NewBSStore(ctx, constants.FormatDefaultString, bs, testMemTableSize, NewUnlimitedMemQuotaProvider())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		return st
	}
	kept := chunks.NewChunk([]byte("kept"))
	dropped := chunks.NewChunk([]byte("dropped"))

	writer := open()
	require.NoError(t, writer.AddTableFilesToManifest(ctx, writeTableOf(t, ctx, writer, kept, dropped), refsTo))
	committed, err := writer.Commit(ctx, kept.Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, committed)

	collector := open()
	keptOnly := writeTableOf(t, ctx, collector, kept)
	before := collector.upstream.gcGen
	require.NoError(t, ReplaceStoreTableFiles(ctx, collector, map[hash.Hash]uint32{hash.Parse(firstKey(keptOnly)): 1}))
	require.NotEqual(t, before, collector.upstream.gcGen)
	require.Equal(t, kept.Hash(), collector.upstream.root)
	require.Len(t, collector.upstream.specs, 1)

	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = writer.AddTableFilesToManifest(bounded, writeTableOf(t, ctx, writer, referencing(dropped)), refsTo)
	require.ErrorIs(t, err, ErrTableFileNotFound)
	require.NoError(t, bounded.Err())

	landed := writeTableOf(t, ctx, writer, referencing(kept))
	require.NoError(t, writer.AddTableFilesToManifest(bounded, landed, refsTo))
	_, contents, _, err := writer.manifestMgr.Fetch(ctx, writer.stats)
	require.NoError(t, err)
	require.Equal(t, collector.upstream.gcGen, contents.gcGen)
	names := map[string]bool{}
	for _, spec := range contents.specs {
		names[spec.name.String()] = true
	}
	require.Equal(t, map[string]bool{firstKey(keptOnly): true, firstKey(landed): true}, names)
}

// TestReplaceStoreTableFilesRefusesWhatItWouldBreak is ReplaceStoreTableFiles
// declining to write: a view another process's write has moved the manifest
// past and chunks the manifest does not name yet, both of which it would drop,
// and files that do not hold the root, which would leave the store unreadable.
func TestReplaceStoreTableFilesRefusesWhatItWouldBreak(t *testing.T) {
	ctx := context.Background()
	bs := blobstore.NewLocalBlobstore(t.TempDir())
	st, err := NewBSStore(ctx, constants.FormatDefaultString, bs, testMemTableSize, NewUnlimitedMemQuotaProvider())
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	root := chunks.NewChunk([]byte("root"))
	rootFile := writeTableOf(t, ctx, st, root)
	require.NoError(t, st.AddTableFilesToManifest(ctx, rootFile, refsTo))
	committed, err := st.Commit(ctx, root.Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, committed)
	replacement := map[hash.Hash]uint32{hash.Parse(firstKey(writeTableOf(t, ctx, st, root, chunks.NewChunk([]byte("more"))))): 2}

	elsewhere := map[hash.Hash]uint32{hash.Parse(firstKey(writeTableOf(t, ctx, st, chunks.NewChunk([]byte("not the root"))))): 1}
	require.ErrorIs(t, ReplaceStoreTableFiles(ctx, st, elsewhere), ErrTableFileNotFound)

	// Another process adds a table file. It writes the blobstore's manifest
	// directly, as a process with its own manifest cache does, so this
	// process's cache still holds the lock st read.
	theirs := writeTableOf(t, ctx, st, chunks.NewChunk([]byte("theirs")))
	moved := st.upstream
	moved.specs = append(append([]tableSpec{}, moved.specs...), tableSpec{name: hash.Parse(firstKey(theirs)), chunkCount: 1})
	moved.lock = generateLockHash(moved.root, moved.specs, moved.appendix, nil)
	written, err := blobstoreManifest{bs}.Update(ctx, st.fatalBehavior, st.upstream.lock, moved, st.stats, nil)
	require.NoError(t, err)
	require.Equal(t, moved.lock, written.lock)

	require.ErrorIs(t, ReplaceStoreTableFiles(ctx, st, replacement), ErrConcurrentManifestWriteDuringOverwrite)
	_, contents, err := manifestVersionAndContents(ctx, bs)
	require.NoError(t, err)
	require.Equal(t, moved.lock, contents.lock)
	require.Equal(t, moved.gcGen, contents.gcGen)

	// The same process again, now moving the root as well: the manifest
	// refuses the root before it compares the lock, and the moved lock is
	// still what the refusal reports.
	require.NoError(t, st.Rebase(ctx))
	newRoot := chunks.NewChunk([]byte("new root"))
	newRootFile := writeTableOf(t, ctx, st, newRoot)
	rooted := st.upstream
	rooted.root = newRoot.Hash()
	rooted.specs = append(append([]tableSpec{}, rooted.specs...), tableSpec{name: hash.Parse(firstKey(newRootFile)), chunkCount: 1})
	rooted.lock = generateLockHash(rooted.root, rooted.specs, rooted.appendix, nil)
	_, err = blobstoreManifest{bs}.Update(ctx, st.fatalBehavior, st.upstream.lock, rooted, st.stats, nil)
	require.NoError(t, err)
	require.ErrorIs(t, ReplaceStoreTableFiles(ctx, st, replacement), ErrConcurrentManifestWriteDuringOverwrite)

	require.NoError(t, st.Rebase(ctx))
	require.NoError(t, st.Put(ctx, chunks.NewChunk([]byte("novel")), noopGetAddrs))
	require.ErrorContains(t, ReplaceStoreTableFiles(ctx, st, replacement), "does not name yet")
}

func firstKey(m map[string]int) string {
	for k := range m {
		return k
	}
	return ""
}
