package sply2

import (
	"bytes"
	"log"

	bolt "github.com/coreos/bbolt"
	"github.com/pgm/sply2/core"
)

// func foo() {
// 	db, err := bolt.Open(path+"/chunkstat.db", 0600, nil)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	err = db.Update(func(tx RTx) error {
// 		_, err := tx.CreateBucketIfNotExists(ChunkStat)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }

type BoltKVStore struct {
	db *bolt.DB
}

type BoltTx struct {
	tx *bolt.Tx
}

type WrappedBucket struct {
	bucket *bolt.Bucket
}

func (b *WrappedBucket) Get(key []byte) []byte {
	return b.bucket.Get(key)
}

func (b *WrappedBucket) ForEachWithPrefix(prefix []byte, callback func(key []byte, value []byte) error) error {
	c := b.bucket.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		err := callback(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *WrappedBucket) Put(key []byte, value []byte) error {
	return b.bucket.Put(key, value)
}

func (b *WrappedBucket) Delete(key []byte) error {
	return b.bucket.Delete(key)
}

func NewBoltDB(filename string, buckets [][]byte) *BoltKVStore {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	db.Update(func(tx *bolt.Tx) error {
		for _, bucketName := range buckets {
			_, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				log.Fatal(err)
			}
		}

		return nil
	})

	return &BoltKVStore{db: db}
}

func (b *BoltKVStore) Update(callback func(core.RWTx) error) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return callback(&BoltTx{tx})
	})
}

func (b *BoltKVStore) View(callback func(core.RTx) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		return callback(&BoltTx{tx})
	})
}

func (b *BoltKVStore) Close() error {
	return b.db.Close()
}

func (tx *BoltTx) RBucket(name []byte) core.RBucket {
	return &WrappedBucket{tx.tx.Bucket(name)}
}

func (tx *BoltTx) WBucket(name []byte) core.WBucket {
	return &WrappedBucket{tx.tx.Bucket(name)}
}
