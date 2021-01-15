package overlay

import (
	"bytes"
	"github.com/ethereum/go-ethereum/ethdb"
)

type Batch struct {
	batch ethdb.Batch
}

func (b *Batch) ValueSize() int {
	return b.batch.ValueSize()
}
func (b *Batch) Write() error {
	return b.batch.Write()
}
func (b *Batch) Reset() {
	b.batch.Reset()
}
func (b *Batch) Replay(w ethdb.KeyValueWriter) error {
	switch w.(type) {
	case *OverlayWrapperDB:
		b.batch.Replay(w)
	default:
		b.batch.Replay(&nonoverlayReplayer{db: w})
	}
	return b.batch.Replay(w)
}
func (b *Batch) Put(key []byte, value []byte) error {
	err := b.batch.Put(key, value)
	if err != nil {
		return err
	}
	return b.batch.Delete(deleted(key))
}
func (b *Batch) Delete(key []byte) error {
	err := b.batch.Delete(key)
	if err != nil {
		return err
	}
	return b.batch.Put(deleted(key), []byte{})
}

type nonoverlayReplayer struct {
	db ethdb.KeyValueWriter
}

func (r *nonoverlayReplayer) Put(key, value []byte) error {
	if !bytes.HasPrefix(key, []byte("deleted/")) {
		return r.db.Put(key, value)
	}
	return nil
}
func (r *nonoverlayReplayer) Delete(key []byte) error {
	if !bytes.HasPrefix(key, []byte("deleted/")) {
		return r.db.Delete(key)
	}
	return nil
}
