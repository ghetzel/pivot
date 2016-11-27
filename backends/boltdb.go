package backends

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/ghetzel/pivot/dal"
	"gopkg.in/vmihailenco/msgpack.v2"
	"os"
)

var DatabaseMode = os.FileMode(0644)
var DatabaseOptions *bolt.Options = nil

// BoltDB ->
//
type BoltBackend struct {
	Backend
	path string
	db   *bolt.DB
}

func NewBoltBackend(connection dal.ConnectionString) *BoltBackend {
	return &BoltBackend{
		path: connection.Dataset(),
	}
}

func (self *BoltBackend) Initialize() error {
	if db, err := bolt.Open(self.path, DatabaseMode, DatabaseOptions); err == nil {
		self.db = db
	} else {
		return err
	}

	return nil
}

func (self *BoltBackend) InsertRecords(collection string, recordset *dal.RecordSet) error {
	return self.upsertRecords(collection, recordset, true)
}

func (self *BoltBackend) GetRecordById(collection string, id dal.Identity) (*dal.Record, error) {
	record := dal.NewRecord(id)

	err := self.db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte(collection[:])); bucket != nil {
			if data := bucket.Get([]byte(id[:])); data != nil {
				return msgpack.Unmarshal(data, record)
			}else{
				return fmt.Errorf("Record %q does not exist", id)
			}
		} else {
			return fmt.Errorf("Failed to retrieve bucket %q", collection)
		}

		return nil
	})

	if err == nil {
		return record, nil
	}else{
		return nil, err
	}
}

func (self *BoltBackend) UpdateRecords(collection string, recordset *dal.RecordSet) error {
	return self.upsertRecords(collection, recordset, false)
}

func (self *BoltBackend) DeleteRecords(collection string, ids []dal.Identity) error {
	return self.db.Update(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte(collection[:])); bucket != nil {
			for _, id := range ids {
				if err := bucket.Delete([]byte(id[:])); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("Failed to retrieve bucket %q", collection)
		}

		return nil
	})
}

func (self *BoltBackend) WithSearch() Searchable {
	return nil
}

func (self *BoltBackend) CreateCollection(definition dal.Collection) error {
	return self.db.Update(func(tx *bolt.Tx) error {
		bucketName := []byte(definition.Name[:])
		_, err := tx.CreateBucket(bucketName)
		return err
	})
}

func (self *BoltBackend) DeleteCollection(collection string) error {
	return self.db.Update(func(tx *bolt.Tx) error {
		bucketName := []byte(collection[:])
		return tx.DeleteBucket(bucketName)
	})
}

func (self *BoltBackend) GetCollection(name string) (dal.Collection, error) {
	collection := dal.NewCollection(name)

	err := self.db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte(name[:])); bucket != nil {
			collection.Name = name
			collection.Properties[`FillPercent`] = bucket.FillPercent
		}else{
			return fmt.Errorf("No such collection %q", name)
		}

		return nil
	})

	return *collection, err
}

func (self *BoltBackend) upsertRecords(collection string, recordset *dal.RecordSet, autocreateBucket bool) error {
	return self.db.Update(func(tx *bolt.Tx) error {
		bucketName := []byte(collection[:])
		bucket := tx.Bucket(bucketName)

		if bucket == nil {
			if autocreateBucket {
				if b, err := tx.CreateBucket(bucketName); err == nil {
					bucket = b
				} else {
					return fmt.Errorf("Failed to create bucket %q: %v", collection, err)
				}
			} else {
				return fmt.Errorf("Failed to retrieve bucket %q", collection)
			}
		}

		for _, record := range recordset.Records {
			if data, err := msgpack.Marshal(record); err == nil {
				if err := bucket.Put([]byte(record.ID[:]), data); err != nil {
					return err
				}
			} else {
				return err
			}
		}

		return nil
	})
}
