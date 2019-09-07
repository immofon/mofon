package uniquedb

import (
	"errors"
	"fmt"
	"log"
	"time"

	"go.etcd.io/bbolt"
)

var ErrExist = errors.New("key exist")

type uniqueDB struct {
	db *bbolt.DB
}

func (u *uniqueDB) Set(namespace string, id string) bool {
	err := u.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(namespace))
		if err != nil {
			return err
		}

		t := bucket.Get([]byte(id))
		if t != nil {
			return ErrExist
		}

		return bucket.Put([]byte(id), []byte(fmt.Sprint(time.Now().Unix())))
	})
	return err == nil
}

func (u *uniqueDB) Exist(namespace string, id string) bool {
	err := u.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(namespace))
		if bucket == nil {
			return ErrExist
		}
		t := bucket.Get([]byte(id))
		if t != nil {
			return nil
		}
		return ErrExist
	})

	return err == nil
}

func (u *uniqueDB) Close() {
	u.db.Close()
}

func BoltDB(path string) Adapter {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	return &uniqueDB{db: db}
}

type Adapter interface {
	Set(namespace string, id string) bool
	Exist(namespace string, id string) bool
	Close()
}
