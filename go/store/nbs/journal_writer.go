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

// NOTICE (Apache License 2.0, section 4(b)): this file was modified in 2026 by
// Brandon Fryslie for the links-issue-tracker (`lit`) project. It is not the
// upstream github.com/dolthub/dolt version. What changed, why, and what would
// let the change be dropped are recorded in the patch ledger that
// README.lit-fork.md at the root of this fork points to.

package nbs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	dherrors "github.com/dolthub/dolt/go/libraries/utils/errors"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	// journalWriterBuffSize is the size of the statically allocated buffer where journal records are
	// built before being written to the journal file on disk. There is not a hard limit on the size
	// of records – specifically, some newer data chunking formats (i.e. optimized JSON storage) can
	// produce chunks (and therefore chunk records) that are megabytes in size. The current limit of
	// 5MB should be large enough to cover all but the most extreme cases.
	journalWriterBuffSize = 5 * 1024 * 1024

	chunkJournalAddr = chunks.JournalFileID

	journalIndexFileName = "journal.idx"

	// journalIndexDefaultMaxNovel determines how often we flush
	// records qto the out-of-band journal index file.
	journalIndexDefaultMaxNovel = 16384

	// journalMaybeSyncThreshold determines how much un-syncd written data
	// can be outstanding to the journal before we will sync it.
	journalMaybeSyncThreshold = 64 * 1024 * 1024
)

var (
	journalAddr = hash.Parse(chunkJournalAddr)
)

func isJournalAddr(h hash.Hash) bool {
	return h == journalAddr
}

func fileExists(path string) (bool, error) {
	var err error
	if path, err = filepath.Abs(path); err != nil {
		return false, err
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if info.IsDir() {
		return true, fmt.Errorf("expected file %s, found directory", path)
	}
	return true, nil
}

func openJournalWriter(ctx context.Context, path string) (wr *journalWriter, exists bool, err error) {
	var f *os.File
	if path, err = filepath.Abs(path); err != nil {
		return nil, false, err
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	} else if info.IsDir() {
		return nil, true, fmt.Errorf("expected file %s found directory", chunkJournalName)
	}
	if f, err = os.OpenFile(path, os.O_RDWR, 0666); err != nil {
		return nil, true, err
	}

	return &journalWriter{
		buf:     make([]byte, 0, journalWriterBuffSize),
		journal: f,
		path:    path,
	}, true, nil
}

// openReadOnlyJournal opens the journal at |path| for reading only: the
// journal of a store that does not hold the manifest lock, which another
// process may be appending to. The writer it returns has no buffer to build
// records in, and its file refuses writes.
func openReadOnlyJournal(ctx context.Context, path string) (wr *journalWriter, exists bool, err error) {
	if path, err = filepath.Abs(path); err != nil {
		return nil, false, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	if info, err := f.Stat(); err != nil {
		return nil, true, errors.Join(err, f.Close())
	} else if info.IsDir() {
		return nil, true, errors.Join(fmt.Errorf("expected file %s found directory", chunkJournalName), f.Close())
	}
	return &journalWriter{journal: f, path: path}, true, nil
}

func createJournalWriter(ctx context.Context, path string) (wr *journalWriter, err error) {
	var f *os.File
	if path, err = filepath.Abs(path); err != nil {
		return nil, err
	}

	_, err = os.Stat(path)
	if err == nil {
		return nil, fmt.Errorf("journal file %s already exists", chunkJournalName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666); err != nil {
		return nil, err
	}

	return &journalWriter{
		buf:     make([]byte, 0, journalWriterBuffSize),
		journal: f,
		path:    path,
	}, nil
}

func deleteJournalAndIndexFiles(ctx context.Context, path string) (err error) {
	if err = os.Remove(path); err != nil {
		return err
	}
	idxPath := filepath.Join(filepath.Dir(path), journalIndexFileName)
	return os.Remove(idxPath)
}

type journalWriter struct {
	ranges      rangeIndex
	journal     *os.File
	indexWriter *bufio.Writer
	index       *os.File
	path        string
	buf         []byte
	indexed     int64
	unsyncd     uint64
	uncmpSz     uint64
	// off indicates the last position that has been written to the journal buffer
	off         int64
	maxNovel    int
	lock        sync.RWMutex
	batchCrc    uint32
	currentRoot hash.Hash
}

var _ io.Closer = &journalWriter{}

// journalFindings is what a read of a journal and its index found, before
// anything acts on it. It is the whole result of the read pass: a read-only
// journal adopts it as it is, and a writable one first repairs the files to
// match it (repairJournal).
type journalFindings struct {
	ranges rangeIndex
	// indexed is the journal offset the index's valid batches cover.
	indexed int64
	// indexEnd is the index file offset just past its last valid batch.
	indexEnd int64
	// tail holds a lookup for each chunk record past |indexed|, in journal
	// order: the lookups the index does not yet hold.
	tail []lookup
	// end is the journal offset just past its last valid record.
	end     int64
	uncmpSz uint64
	// last is the root of the journal's last root hash record, and
	// lastOffset that record's offset.
	last       hash.Hash
	lastOffset int64
	// roots are the journal's root hash records past |indexed|, in journal
	// order, for the reflog.
	roots []reflogRootHashEntry
}

// errCorruptJournalIndex marks a read pass that failed on the journal index
// rather than on the journal: the index can be discarded and rebuilt from the
// journal, which is the source of truth.
var errCorruptJournalIndex = errors.New("corrupt journal index")

// readJournal reads |index| (nil when there is none) up to its last valid
// batch, then |journal| from the offset that batch covers to its last valid
// record, and returns what it found. It writes nothing, to either file: what
// to do about a garbage tail, an index tail or an index past its last valid
// batch is repairJournal's, and only a journal that holds the manifest lock
// runs it.
func readJournal(ctx context.Context, journal *os.File, index *os.File, warningsCb func(error)) (f journalFindings, err error) {
	f.ranges = newRangeIndex()

	if index != nil {
		var info os.FileInfo
		if info, err = index.Stat(); err != nil {
			return journalFindings{}, err
		}

		// initialize range index with enough capacity to
		// avoid rehashing during bootstrapping
		cnt := estimateRangeCount(info)
		f.ranges.cached = make(map[addr16]Range, cnt)

		eg, ectx := errgroup.WithContext(ctx)
		ch := make(chan []lookup, 4)

		// process the indexed portion of the journal
		var prev int64

		eg.Go(func() error {
			defer close(ch)
			var perr error
			f.indexEnd, perr = processIndexRecords(bufio.NewReader(index), info.Size(), func(m lookupMeta, batch []lookup, batchChecksum uint32) error {
				if m.checkSum != batchChecksum {
					return fmt.Errorf("invalid index checksum (%d != %d)", batchChecksum, m.checkSum)
				}

				if m.batchStart != prev {
					return fmt.Errorf("index records do not cover contiguous region (%d != %d)", m.batchStart, prev)
				}
				prev = m.batchEnd

				// |r.end| is expected to point to a root hash record in |journal|
				// containing a hash equal to |r.lastRoot|, validate this here
				if h, err := peekRootHashAt(journal, int64(m.batchEnd)); err != nil {
					return err
				} else if h != m.latestHash {
					return fmt.Errorf("invalid index record hash (%s != %s)", h.String(), m.latestHash.String())
				}

				select {
				case <-ectx.Done():
					return ectx.Err()
				case ch <- batch:
					// record a high-water-mark for the indexed portion of the journal
					f.indexed = int64(m.batchEnd)
				}
				return nil
			})
			return perr
		})
		// populate range hashmap
		eg.Go(func() error {
			for {
				select {
				case <-ectx.Done():
					return nil
				case ll, ok := <-ch:
					if !ok {
						return nil
					}
					for _, l := range ll {
						f.ranges.putCached(l.a, l.r)
					}
				}
			}
		})

		if err = eg.Wait(); err != nil {
			// A cancelled read says nothing about the index.
			if cerr := context.Cause(ctx); cerr != nil {
				return journalFindings{}, cerr
			}
			return journalFindings{}, fmt.Errorf("error bootstrapping chunk journal: %s: %w", err.Error(), errCorruptJournalIndex)
		}
		f.ranges = f.ranges.flatten(ctx)
	}

	// process the non-indexed portion of the journal starting at |f.indexed|,
	// at minimum the non-indexed portion will include a root hash record.
	f.end, err = processJournalRecords(ctx, journal, false /* tryTruncate */, f.indexed, func(o int64, r journalRec) error {
		switch r.kind {
		case chunkJournalRecKind:
			rng := Range{
				Offset: uint64(o) + uint64(r.payloadOffset()),
				Length: uint32(len(r.payload)),
			}
			f.ranges.put(r.address, rng)
			f.uncmpSz += r.uncompressedPayloadSize()
			f.tail = append(f.tail, lookup{a: toAddr16(r.address), r: rng})

		case rootHashJournalRecKind:
			f.lastOffset = o
			f.last = r.address
			f.roots = append(f.roots, reflogRootHashEntry{
				root:      r.address.String(),
				timestamp: r.timestamp,
			})

		default:
			return fmt.Errorf("unknown journal record kind (%d)", r.kind)
		}
		return nil
	}, warningsCb)
	if err != nil {
		return journalFindings{}, err
	}
	return f, nil
}

// adopt takes |f| as the writer's state: the ranges it can serve, the journal
// offset it appends at, and the root it last committed. The roots |f| read
// are pushed to |reflogRingBuffer|, which maintains the most recently updated
// roots used to generate the reflog.
func (wr *journalWriter) adopt(f journalFindings, reflogRingBuffer *reflogRingBuffer) {
	if !reflogDisabled && reflogRingBuffer != nil {
		for _, r := range f.roots {
			reflogRingBuffer.Push(r)
		}
	}
	if wr.maxNovel == 0 {
		wr.maxNovel = journalIndexDefaultMaxNovel
	}
	wr.ranges = f.ranges
	wr.indexed = f.indexed
	wr.off = f.end
	wr.uncmpSz = f.uncmpSz
	wr.currentRoot = f.last
}

// bootstrapJournal opens the journal index read-write (creating it when there
// is none), reads both files (readJournal) and repairs them to match what it
// found (repairJournal): the bootstrap of a journal that holds the manifest
// lock. It returns the most recent root hash for the journal.
func (wr *journalWriter) bootstrapJournal(ctx context.Context, reflogRingBuffer *reflogRingBuffer, warningsCb func(error)) (last hash.Hash, err error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()

	p := filepath.Join(filepath.Dir(wr.path), journalIndexFileName)
	var ok bool
	ok, err = fileExists(p)
	if err != nil {
		return
	}
	if wr.index, err = os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return
	}
	wr.indexWriter = bufio.NewWriterSize(wr.index, journalIndexDefaultMaxNovel)
	// A newly created index holds nothing to read.
	readFrom := wr.index
	if !ok {
		readFrom = nil
	}

	f, err := readJournal(ctx, wr.journal, readFrom, warningsCb)
	if errors.Is(err, errCorruptJournalIndex) {
		// The open fails, as it always has, and the index it could not read is
		// emptied so the next open rebuilds it from the journal.
		if cerr := wr.truncateIndex(0); cerr != nil {
			return hash.Hash{}, fmt.Errorf("error recovering corrupted chunk journal index: %s", err.Error())
		}
		return hash.Hash{}, err
	} else if err != nil {
		return hash.Hash{}, err
	}
	wr.adopt(f, reflogRingBuffer)
	if err = wr.repairJournal(ctx, f); err != nil {
		return hash.Hash{}, err
	}
	return f.last, nil
}

// bootstrapReadOnlyJournal reads the journal and its index, if there is one,
// and adopts what it found: the bootstrap of a journal that does not hold the
// manifest lock. Both files are read through read-only handles, and the index
// handle is closed before it returns, so the writer it leaves holds no index
// writer: nothing this journal does can write either file. An index it cannot
// read is passed over and the journal read whole, since the journal is the
// source of truth and the index is only an accelerator.
func (wr *journalWriter) bootstrapReadOnlyJournal(ctx context.Context, reflogRingBuffer *reflogRingBuffer, warningsCb func(error)) (last hash.Hash, err error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()

	index, err := os.Open(filepath.Join(filepath.Dir(wr.path), journalIndexFileName))
	if errors.Is(err, os.ErrNotExist) {
		index, err = nil, nil
	} else if err != nil {
		return hash.Hash{}, err
	} else {
		defer func() {
			if cerr := index.Close(); err == nil {
				err = cerr
			}
		}()
	}

	f, err := readJournal(ctx, wr.journal, index, warningsCb)
	if errors.Is(err, errCorruptJournalIndex) {
		if warningsCb != nil {
			warningsCb(fmt.Errorf("a read-only journal open passed over its index and read the whole journal: %w", err))
		}
		f, err = readJournal(ctx, wr.journal, nil, warningsCb)
	}
	if err != nil {
		return hash.Hash{}, err
	}
	wr.adopt(f, reflogRingBuffer)
	return f.last, nil
}

// repairJournal makes the journal and its index match |f|, which readJournal
// found in them: it truncates whatever follows the journal's last valid
// record (historically null bytes, sometimes garbage; readJournal has already
// refused a tail that looks like lost data) and syncs, rewinds the index to
// its last valid batch, writes the lookups the index does not yet hold, and
// saves that progress as an index record once there are more than
// |wr.maxNovel| of them. Only a journal that holds the manifest lock runs it.
func (wr *journalWriter) repairJournal(ctx context.Context, f journalFindings) error {
	if err := wr.journal.Truncate(f.end); err != nil {
		return err
	}
	if err := wr.journal.Sync(); err != nil {
		return err
	}
	// rewind index to last safe point. Note that |f.indexEnd| refers
	// to a location in the index file, while |f.indexed| refers to a position
	// in the journal file.
	if err := wr.truncateIndex(f.indexEnd); err != nil {
		return err
	}
	for _, l := range f.tail {
		if err := writeIndexLookup(wr.indexWriter, l); err != nil {
			return err
		}
		wr.batchCrc = crc32.Update(wr.batchCrc, crcTable, l.a[:])
	}
	if wr.ranges.novelCount() > wr.maxNovel {
		// save bootstrap progress
		if err := wr.flushIndexRecord(ctx, f.last, f.lastOffset); err != nil {
			return err
		}
	}
	return nil
}

func (wr *journalWriter) truncateIndex(off int64) error {
	if _, err := wr.index.Seek(off, io.SeekStart); err != nil {
		return err
	}
	if err := wr.index.Truncate(off); err != nil {
		return err
	}
	return nil
}

// hasAddr returns true if the journal contains a chunk with addr |h|.
func (wr *journalWriter) hasAddr(h hash.Hash) (ok bool) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	_, ok = wr.ranges.get(h)
	return
}

// getCompressedChunk reads the CompressedChunks with addr |h|.
func (wr *journalWriter) getCompressedChunk(h hash.Hash) (CompressedChunk, error) {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	r, ok := wr.ranges.get(h)
	if !ok {
		return CompressedChunk{}, nil
	}
	buf := make([]byte, r.Length)
	if _, err := wr.readAt(buf, int64(r.Offset)); err != nil {
		return CompressedChunk{}, err
	}
	return NewCompressedChunk(hash.Hash(h), buf)
}

// getCompressedChunk reads the CompressedChunks with addr |h|.
func (wr *journalWriter) getCompressedChunkAtRange(r Range, h hash.Hash) (CompressedChunk, error) {
	buf := make([]byte, r.Length)
	if _, err := wr.readAt(buf, int64(r.Offset)); err != nil {
		return CompressedChunk{}, err
	}
	return NewCompressedChunk(hash.Hash(h), buf)
}

// getRange returns a Range for the chunk with addr |h|.
func (wr *journalWriter) getRange(ctx context.Context, behavior dherrors.FatalBehavior, h hash.Hash) (rng Range, ok bool, err error) {
	// callers will use |rng| to read directly from the
	// journal file, so we must flush here
	if err = wr.maybeFlush(ctx, behavior); err != nil {
		return
	}
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	rng, ok = wr.ranges.get(h)
	return
}

// writeCompressedChunk writes |cc| to the journal.
func (wr *journalWriter) writeCompressedChunk(ctx context.Context, behavior dherrors.FatalBehavior, cc CompressedChunk) error {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	recordLen, payloadOff := chunkRecordSize(cc)
	rng := Range{
		Offset: uint64(wr.offset()) + uint64(payloadOff),
		Length: uint32(len(cc.FullCompressedChunk)),
	}
	buf, err := wr.getBytes(ctx, behavior, int(recordLen))
	if err != nil {
		return err
	}
	wr.unsyncd += uint64(recordLen)
	_ = writeChunkRecord(buf, cc)
	wr.ranges.put(cc.H, rng)

	a := toAddr16(cc.H)
	if err := writeIndexLookup(wr.indexWriter, lookup{a: a, r: rng}); err != nil {
		return err
	}
	wr.batchCrc = crc32.Update(wr.batchCrc, crcTable, a[:])

	// To fulfill our durability guarantees, we technically only need to
	// file.Sync() the journal when we commit a new root chunk. However,
	// allowing an unbounded amount of unflushed dirty pages to accumulate
	// in the OS's page cache makes it possible for small writes which come
	// along during a large non-committing write to block on flushing all
	// of the unflushed data. To minimize interference from large
	// non-committing writes, we cap the amount of unflushed data here.
	//
	// We go through |commitRootHash|, instead of directly |Sync()|ing the
	// file, because we also have accumulating delayed work in the form of
	// journal index records which may need to be serialized and flushed.
	// Assumptions in journal bootstrapping and the contents of the journal
	// index require us to have a newly written root hash record anytime we
	// write index records out. It's perfectly fine to reuse the current
	// root hash, and this will also take care of the |Sync|.
	if wr.unsyncd > journalMaybeSyncThreshold && !wr.currentRoot.IsEmpty() {
		return wr.commitRootHashUnlocked(ctx, behavior, wr.currentRoot)
	}

	return nil
}

// commitRootHash commits |root| to the journal and syncs the file to disk.
func (wr *journalWriter) commitRootHash(ctx context.Context, behavior dherrors.FatalBehavior, root hash.Hash) error {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	return wr.commitRootHashUnlocked(ctx, behavior, root)
}

func (wr *journalWriter) size() int64 {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	return wr.off
}

func (wr *journalWriter) commitRootHashUnlocked(ctx context.Context, behavior dherrors.FatalBehavior, root hash.Hash) error {
	defer trace.StartRegion(ctx, "commit-root").End()

	buf, err := wr.getBytes(ctx, behavior, rootHashRecordSize())
	if err != nil {
		return err
	}
	wr.currentRoot = root
	n := writeRootHashRecord(buf, root)
	if err = wr.flush(ctx, behavior); err != nil {
		return err
	}
	func() {
		defer trace.StartRegion(ctx, "sync").End()

		err = wr.journal.Sync()
	}()
	if err != nil {
		return dherrors.Fatalf(behavior, "%w: error syncing journal", err)
	}

	wr.unsyncd = 0
	if wr.ranges.novelCount() > wr.maxNovel {
		o := wr.offset() - int64(n) // pre-commit journal offset
		if err := wr.flushIndexRecord(ctx, root, o); err != nil {
			return err
		}
	}
	return nil
}

// flushIndexRecord writes metadata for a range of index lookups to the
// out-of-band journal index file. Index records accelerate journal
// bootstrapping by reducing the amount of the journal that must be processed.
func (wr *journalWriter) flushIndexRecord(ctx context.Context, root hash.Hash, end int64) (err error) {
	defer trace.StartRegion(ctx, "flushIndexRecord").End()
	if err := writeJournalIndexMeta(wr.indexWriter, root, wr.indexed, end, wr.batchCrc); err != nil {
		return err
	}
	wr.batchCrc = 0
	wr.ranges = wr.ranges.flatten(ctx)
	// set a new high-water-mark for the indexed portion of the journal
	wr.indexed = end
	return
}

// readAt reads len(p) bytes from the journal at offset |off|.
func (wr *journalWriter) readAt(p []byte, off int64) (n int, err error) {
	var bp []byte
	if off < wr.off {
		// fill some or all of |p| from |wr.file|
		fread := int(wr.off - off)
		if len(p) > fread {
			// straddled read
			bp = p[fread:]
			p = p[:fread]
		}
		if n, err = wr.journal.ReadAt(p, off); err != nil {
			return 0, err
		}
		off = 0
	} else {
		// fill all of |p| from |wr.buf|
		bp = p
		off -= wr.off
	}
	n += copy(bp, wr.buf[off:])
	return
}

// getBytes returns a buffer for writers to copy data into.
func (wr *journalWriter) getBytes(ctx context.Context, behavior dherrors.FatalBehavior, n int) (buf []byte, err error) {
	c, l := cap(wr.buf), len(wr.buf)
	if n > c {
		err = fmt.Errorf("requested bytes (%d) exceeds capacity (%d)", n, c)
		return
	} else if n > c-l {
		if err = wr.flush(ctx, behavior); err != nil {
			return
		}
	}
	l = len(wr.buf)
	wr.buf = wr.buf[:l+n]
	buf = wr.buf[l : l+n]
	return
}

// flush writes buffered data into the journal file.
func (wr *journalWriter) flush(ctx context.Context, behavior dherrors.FatalBehavior) (err error) {
	defer trace.StartRegion(ctx, "flush journal").End()
	if _, err = wr.journal.WriteAt(wr.buf, wr.off); err != nil {
		return dherrors.Fatalf(behavior, "%w: error writing to database journal file", err)
	}
	wr.off += int64(len(wr.buf))
	wr.buf = wr.buf[:0]
	return
}

// maybeFlush flushes buffered data, if any exists.
func (wr *journalWriter) maybeFlush(ctx context.Context, behavior dherrors.FatalBehavior) (err error) {
	wr.lock.RLock()
	empty := len(wr.buf) == 0
	wr.lock.RUnlock()
	if empty {
		return
	}
	wr.lock.Lock()
	defer wr.lock.Unlock()
	return wr.flush(ctx, behavior)
}

type journalWriterSnapshot struct {
	io.Reader
	closer func() error
}

func (s journalWriterSnapshot) Close() error {
	return s.closer()
}

// snapshot returns an io.Reader with a consistent view of
// the current state of the journal file.
func (wr *journalWriter) snapshot(ctx context.Context, behavior dherrors.FatalBehavior) (io.ReadCloser, int64, error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()
	if err := wr.flush(ctx, behavior); err != nil {
		return nil, 0, err
	}
	// open a new file descriptor with an
	// independent lifecycle from |wr.file|
	f, err := os.Open(wr.path)
	if err != nil {
		return nil, 0, err
	}
	return journalWriterSnapshot{
		io.LimitReader(f, wr.off),
		func() error {
			return f.Close()
		},
	}, wr.off, nil
}

func (wr *journalWriter) offset() int64 {
	return wr.off + int64(len(wr.buf))
}

func (wr *journalWriter) currentSize() int64 {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	return wr.offset()
}

func (wr *journalWriter) uncompressedSize() uint64 {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	return wr.uncmpSz
}

func (wr *journalWriter) recordCount() uint32 {
	wr.lock.RLock()
	defer wr.lock.RUnlock()
	return wr.ranges.count()
}

func (wr *journalWriter) Close() (err error) {
	wr.lock.Lock()
	defer wr.lock.Unlock()

	if wr.journal == nil {
		logrus.Warnf("journal writer has already been closed (%s)", wr.path)
		return nil
	}

	// Let caller fatal on Close.
	if err = wr.flush(context.Background(), dherrors.FatalBehaviorError); err != nil {
		return err
	}
	if wr.index != nil {
		_ = wr.indexWriter.Flush()
		_ = wr.index.Close()
	}
	// Only what was written since the last sync needs one: a journal that
	// wrote nothing, a read-only one always among them, closes without it.
	if wr.unsyncd > 0 {
		if cerr := wr.journal.Sync(); cerr != nil {
			err = cerr
		}
	}
	if cerr := wr.journal.Close(); cerr != nil {
		err = cerr
	} else {
		// Nil out the journal after the file has been closed, so that it's obvious it's been closed
		wr.journal = nil
	}

	return err
}

// A rangeIndex maps chunk addresses to read Ranges in the chunk journal file.
type rangeIndex struct {
	// novel Ranges represent most recent chunks written to
	// the journal. These Ranges have not yet been written to
	// a journal index record.
	novel map[hash.Hash]Range

	// cached Ranges are bootstrapped from an out-of-band journal
	// index file. To save memory, these Ranges are keyed by a 16-byte
	// prefix of their addr which is assumed to be globally unique
	cached map[addr16]Range
}

type addr16 [16]byte

func toAddr16(full hash.Hash) (prefix addr16) {
	copy(prefix[:], full[:])
	return
}

func newRangeIndex() rangeIndex {
	return rangeIndex{
		novel:  make(map[hash.Hash]Range, journalIndexDefaultMaxNovel),
		cached: make(map[addr16]Range),
	}
}

func estimateRangeCount(info os.FileInfo) uint32 {
	return uint32(info.Size()/32) + journalIndexDefaultMaxNovel
}

func (idx rangeIndex) get(h hash.Hash) (rng Range, ok bool) {
	rng, ok = idx.novel[h]
	if !ok {
		rng, ok = idx.cached[toAddr16(h)]
	}
	return
}

func (idx rangeIndex) put(h hash.Hash, rng Range) {
	idx.novel[h] = rng
}

func (idx rangeIndex) putCached(a addr16, rng Range) {
	idx.cached[a] = rng
}

func (idx rangeIndex) count() uint32 {
	return uint32(len(idx.novel) + len(idx.cached))
}

func (idx rangeIndex) novelCount() int {
	return len(idx.novel)
}

func (idx rangeIndex) flatten(ctx context.Context) rangeIndex {
	defer trace.StartRegion(ctx, "flatten journal index").End()
	trace.Logf(ctx, "map index cached count", "%d", len(idx.cached))
	trace.Logf(ctx, "map index novel count", "%d", len(idx.novel))
	for a, r := range idx.novel {
		idx.cached[toAddr16(a)] = r
	}
	idx.novel = make(map[hash.Hash]Range, journalIndexDefaultMaxNovel)
	return idx
}
