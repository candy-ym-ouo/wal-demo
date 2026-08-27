package wal

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testOptions() Options {
	return Options{
		SyncPolicy:      SyncBatch,
		BatchSize:       1 << 20,
		FlushInterval:   time.Hour,
		MaxSegmentBytes: 300,
		MaxRecordBytes:  32,
		MaxPayloadBytes: 1 << 20,
	}
}

func TestAppendReadReplayAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()
	opts.MaxSegmentBytes = 240
	w, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]byte{
		[]byte("short"),
		bytes.Repeat([]byte("fragmented-value-"), 8),
		[]byte("last"),
	}
	for i, payload := range want {
		seq, err := w.AppendBatch(payload)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("sequence = %d, want %d", seq, i+1)
		}
	}
	if durable, err := w.Sync(); err != nil || durable != 3 {
		t.Fatalf("sync = %d, %v", durable, err)
	}
	for i, expected := range want {
		got, err := w.Read(uint64(i + 1))
		if err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("read %d = %q, %v", i+1, got, err)
		}
	}
	if len(w.SegmentInfo()) < 2 {
		t.Fatal("expected segment rotation")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var replayed [][]byte
	stats, err := w.Replay(func(_ uint64, payload []byte) error {
		replayed = append(replayed, append([]byte(nil), payload...))
		return nil
	})
	if err != nil || stats.Records != 3 {
		t.Fatalf("replay = %+v, %v", stats, err)
	}
	for i := range want {
		if !bytes.Equal(replayed[i], want[i]) {
			t.Fatalf("replay %d mismatch", i)
		}
	}
	verified, err := w.Verify()
	if err != nil || !verified.Valid || verified.Checked != 3 {
		t.Fatalf("verify = %+v, %v", verified, err)
	}
}

func TestRecoveryTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendSync([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	path := w.cur.path
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x57, 0x41, flagFirst}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w, err = Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.LastSeq() != 1 {
		t.Fatalf("last sequence = %d", w.LastSeq())
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(headerSize+len("durable")) {
		t.Fatalf("tail not truncated: %d", info.Size())
	}
}

func TestChecksumCorruptionIsLocated(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendSync([]byte("checksum me")); err != nil {
		t.Fatal(err)
	}
	if err := w.Corrupt(1); err != nil {
		t.Fatal(err)
	}
	result, err := w.Verify()
	if err == nil || result.Valid || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verify = %+v, %v", result, err)
	}
	var corruption *CorruptionError
	if !errors.As(err, &corruption) || corruption.Offset != 0 {
		t.Fatalf("corruption location = %#v", corruption)
	}
	_ = w.Close()
	if _, err := Open(dir, testOptions()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reopen error = %v", err)
	}
}

func TestCleanOldSegmentsCanReopen(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()
	opts.MaxSegmentBytes = 90
	w, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := w.AppendSync(bytes.Repeat([]byte{byte(i)}, 30)); err != nil {
			t.Fatal(err)
		}
	}
	starts := w.SegmentInfo()
	if len(starts) < 3 {
		t.Fatalf("segments = %d", len(starts))
	}
	before := starts[len(starts)-1].StartSeq
	if err := w.CleanOldSegments(before); err != nil {
		t.Fatal(err)
	}
	entry, err := w.NewReaderFrom(1).Next()
	if err != nil || entry.Seq != before {
		t.Fatalf("reader after cleanup = seq %d, %v", entry.Seq, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := os.Stat(filepath.Join(dir, segmentName(before))); err != nil {
		t.Fatal(err)
	}
}

func TestSyncRetriesAfterFsyncFailure(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	originalSync := syncFile
	failOnce := true
	syncFile = func(file interface{ Sync() error }) error {
		if failOnce {
			failOnce = false
			return errors.New("injected fsync failure")
		}
		return file.Sync()
	}
	defer func() { syncFile = originalSync }()
	seq, err := w.AppendBatch([]byte("retry-me"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Sync(); err == nil {
		t.Fatal("first sync unexpectedly succeeded")
	}
	if w.DurableSeq() != 0 {
		t.Fatalf("durable advanced after failed fsync: %d", w.DurableSeq())
	}
	if durable, err := w.Sync(); err != nil || durable != seq {
		t.Fatalf("retry sync = %d, %v", durable, err)
	}
}

func TestFailedRecoverKeepsExistingIndex(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.AppendSync([]byte("still-readable")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendSync([]byte("will-corrupt")); err != nil {
		t.Fatal(err)
	}
	if err := w.Corrupt(2); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Recover(nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("recover error = %v", err)
	}
	data, err := w.Read(1)
	if err != nil || string(data) != "still-readable" {
		t.Fatalf("read after failed recovery = %q, %v", data, err)
	}
}

func TestCorruptEmptyRecordDoesNotDamageNextRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.AppendSync(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendSync([]byte("next-record")); err != nil {
		t.Fatal(err)
	}
	if err := w.Corrupt(1); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Read(1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("empty record corruption error = %v", err)
	}
	data, err := w.Read(2)
	if err != nil || string(data) != "next-record" {
		t.Fatalf("next record = %q, %v", data, err)
	}
}

func TestRecoveryRejectsSegmentSequenceGap(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendSync([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	gap := filepath.Join(dir, segmentName(99))
	if err := os.WriteFile(gap, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, testOptions()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("segment gap error = %v", err)
	}
}

func TestOversizedLogicalRecordGetsDedicatedSegment(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions()
	opts.MaxSegmentBytes = 100
	w, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.AppendSync([]byte("small")); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("large-record"), 20)
	seq, err := w.AppendSync(payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.Read(seq)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("oversized read length=%d, err=%v", len(got), err)
	}
	segments := w.SegmentInfo()
	if len(segments) != 2 || segments[1].Bytes <= opts.MaxSegmentBytes {
		t.Fatalf("segments = %+v", segments)
	}
}

func TestZeroOptionsUseDocumentedBatchPolicy(t *testing.T) {
	opts, err := (Options{}).Validate()
	if err != nil {
		t.Fatal(err)
	}
	if opts.SyncPolicy != SyncBatch {
		t.Fatalf("zero options policy = %s", opts.PolicyName())
	}
}

func TestReadRangeHonorsBoundsAndContext(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, value := range []string{"one", "two", "three"} {
		if _, err := w.AppendSync([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := w.ReadRange(context.Background(), 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Seq != 2 || string(entries[1].Data) != "three" {
		t.Fatalf("unexpected range: %+v", entries)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	entries, err = w.ReadRange(ctx, 1, 2)
	if !errors.Is(err, context.Canceled) || len(entries) != 0 {
		t.Fatalf("expected canceled empty read, entries=%+v err=%v", entries, err)
	}
}
