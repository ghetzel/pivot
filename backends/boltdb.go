package backends

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/ghetzel/pivot/dal"
	"gopkg.in/vmihailenco/msgpack.v2"
	"os"
	"path"
)

var DatabaseMode = os.FileMode(0644)
var DatabaseOptions *bolt.Options = nil
var DefaultSearchIndexer = `bleve:///`
var DatabaseIndexSubdirectory = `indexes`

// BoltDB ->
//
type BoltBackend struct {
	Backend
	conn        dal.ConnectionString
	db          *bolt.DB
	indexerConn dal.ConnectionString
	indexer     Indexer
}

func NewBoltBackend(connection dal.ConnectionString) *BoltBackend {
	return &BoltBackend{
		conn: connection,
	}
}

func (self *BoltBackend) GetConnectionString() *dal.ConnectionString {
	return &self.conn
}

func (self *BoltBackend) Initialize() error {
	dbBaseDir := self.conn.Dataset()
	dbFileName := `data.boltdb`

	if db, err := bolt.Open(path.Join(dbBaseDir, dbFileName), DatabaseMode, DatabaseOptions); err == nil {
		self.db = db
	} else {
		return err
	}

	if ixConn := self.conn.OptString(`indexer`, DefaultSearchIndexer); ixConn != `` {
		if ics, err := dal.ParseConnectionString(ixConn); err == nil {
			// an empty path denotes using the same parent directory as the DB we're indexing
			if ics.Dataset() == `/` {
				ics.URI.Path = path.Join(dbBaseDir, DatabaseIndexSubdirectory)
			}

			if indexer, err := MakeIndexer(ics); err == nil {
				if err := indexer.Initialize(self); err == nil {
					self.indexerConn = ics
					self.indexer = indexer
					log.Debugf("Search indexing enabled for %T backend at %q", self, self.indexerConn.String())
				} else {
					return err
				}
			} else {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func (self *BoltBackend) InsertRecords(collection string, recordset *dal.RecordSet) error {
	return self.upsertRecords(collection, recordset, true)
}

func (self *BoltBackend) GetRecordById(collection string, id string) (*dal.Record, error) {
	record := dal.NewRecord(id)

	err := self.db.View(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte(collection[:])); bucket != nil {
			if data := bucket.Get([]byte(id[:])); data != nil {
				return msgpack.Unmarshal(data, record)
			} else {
				return fmt.Errorf("Record %q does not exist", id)
			}
		} else {
			return fmt.Errorf("Failed to retrieve bucket %q", collection)
		}

		return nil
	})

	if err == nil {
		return record, nil
	} else {
		return nil, err
	}
}

func (self *BoltBackend) UpdateRecords(collection string, recordset *dal.RecordSet) error {
	return self.upsertRecords(collection, recordset, false)
}

func (self *BoltBackend) DeleteRecords(collection string, ids []string) error {
	return self.db.Update(func(tx *bolt.Tx) error {
		if bucket := tx.Bucket([]byte(collection[:])); bucket != nil {
			for _, id := range ids {
				if err := bucket.Delete([]byte(id[:])); err != nil {
					return err
				}
			}

			// if we have a search index, update it now
			if search := self.WithSearch(); search != nil {
				if err := search.Remove(collection, ids); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("Failed to retrieve bucket %q", collection)
		}

		return nil
	})
}

func (self *BoltBackend) WithSearch() Indexer {
	return self.indexer
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
		} else {
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

		// if we have a search index, update it now
		if search := self.WithSearch(); search != nil {
			if err := search.Index(collection, recordset); err != nil {
				return err
			}
		}

		return nil
	})
}
