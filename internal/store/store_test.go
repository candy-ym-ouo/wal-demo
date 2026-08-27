package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"wal-demo/internal/wal"
)

func openTestStore(t *testing.T, dir string) (*wal.WAL, *Store) {
	t.Helper()
	opts := wal.Options{
		SyncPolicy:      wal.SyncBatch,
		BatchSize:       1 << 20,
		FlushInterval:   time.Hour,
		MaxSegmentBytes: 1 << 20,
		MaxRecordBytes:  64,
		MaxPayloadBytes: 1 << 20,
	}
	log, err := wal.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	st, err := New(log, dir)
	if err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	return log, st
}

func TestStoreSnapshotAndReplay(t *testing.T) {
	dir := t.TempDir()
	log, st := openTestStore(t, dir)
	if _, err := st.Set("alpha", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Set("beta", []byte("two")); err != nil {
		t.Fatal(err)
	}
	watermark, err := st.Snapshot()
	if err != nil || watermark != 2 {
		t.Fatalf("snapshot = %d, %v", watermark, err)
	}
	if _, err := st.Delete("alpha"); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	log, st = openTestStore(t, dir)
	defer log.Close()
	if _, err := st.Get("alpha"); err != ErrKeyNotFound {
		t.Fatalf("deleted key error = %v", err)
	}
	value, err := st.Get("beta")
	if err != nil || !bytes.Equal(value, []byte("two")) {
		t.Fatalf("beta = %q, %v", value, err)
	}
	if st.AppliedSeq() != 3 {
		t.Fatalf("applied = %d", st.AppliedSeq())
	}
}

func TestSetBatchUsesOneExplicitSync(t *testing.T) {
	dir := t.TempDir()
	log, st := openTestStore(t, dir)
	defer log.Close()
	before := log.Metrics().Fsyncs
	seqs, err := st.SetBatch([]Pair{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	})
	if err != nil || len(seqs) != 3 {
		t.Fatalf("batch = %v, %v", seqs, err)
	}
	after := log.Metrics().Fsyncs
	if after-before != 1 {
		t.Fatalf("fsync delta = %d", after-before)
	}
	if st.Count() != 3 || log.DurableSeq() != seqs[2] {
		t.Fatalf("count=%d durable=%d", st.Count(), log.DurableSeq())
	}
}

func TestBrokenSnapshotFallsBackToFullWAL(t *testing.T) {
	dir := t.TempDir()
	log, st := openTestStore(t, dir)
	if _, err := st.Set("survivor", []byte("from-wal")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "snapshot.dat")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	log, st = openTestStore(t, dir)
	defer log.Close()
	value, err := st.Get("survivor")
	if err != nil || string(value) != "from-wal" {
		t.Fatalf("fallback value = %q, %v", value, err)
	}
}

func TestConcurrentMutationsMatchReplayState(t *testing.T) {
	dir := t.TempDir()
	log, st := openTestStore(t, dir)
	const workers = 12
	const writesPerWorker = 40
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				key := fmt.Sprintf("key-%02d", i%7)
				value := []byte(fmt.Sprintf("worker-%02d-write-%02d", worker, i))
				if _, err := st.Set(key, value); err != nil {
					t.Errorf("set: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	before := st.Items()
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log, st = openTestStore(t, dir)
	defer log.Close()
	after := st.Items()
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("live state differs from replay\nlive: %v\nreplay: %v", before, after)
	}
}

func TestItemsPageIsSortedAndBounded(t *testing.T) {
	log, st := openTestStore(t, t.TempDir())
	defer log.Close()
	for _, key := range []string{"charlie", "alpha", "bravo"} {
		if _, err := st.Set(key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	page := st.ItemsPage(1, 1)
	if page.Total != 3 || page.Offset != 1 || page.Limit != 1 {
		t.Fatalf("unexpected page metadata: %+v", page)
	}
	if len(page.Items) != 1 || page.Items[0].Key != "bravo" {
		t.Fatalf("unexpected page items: %+v", page.Items)
	}
	if got := st.ItemsPage(99, 5); len(got.Items) != 0 || got.Offset != 3 {
		t.Fatalf("expected empty tail page, got %+v", got)
	}
}

func TestNamedSnapshotCatalogValidatesAndDeletesArchives(t *testing.T) {
	dir := t.TempDir()
	log, st := openTestStore(t, dir)
	defer log.Close()
	if _, err := st.Set("alpha", []byte("one")); err != nil {
		t.Fatal(err)
	}
	info, err := st.SaveSnapshot("before-delete")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "before-delete" || info.Applied != 1 || info.Keys != 1 || info.Bytes == 0 || info.Checksum == "" {
		t.Fatalf("unexpected snapshot info: %+v", info)
	}
	loaded, err := st.InspectSnapshot("before-delete")
	if err != nil || loaded != info {
		t.Fatalf("inspect = %+v, %v", loaded, err)
	}
	if _, err := st.SaveSnapshot("after_delete"); err != nil {
		t.Fatal(err)
	}
	list, err := st.ListSnapshots()
	if err != nil || len(list) != 2 || list[0].Name != "after_delete" || list[1].Name != "before-delete" {
		t.Fatalf("list = %+v, %v", list, err)
	}
	if _, err := st.SaveSnapshot("../escape"); err != errInvalidSnapshotName {
		t.Fatalf("invalid name error = %v", err)
	}
	if err := st.DeleteSnapshot("before-delete"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InspectSnapshot("before-delete"); !os.IsNotExist(err) {
		t.Fatalf("inspect deleted snapshot error = %v", err)
	}
}
