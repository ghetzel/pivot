package backends

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
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

func (self *BoltBackend) InsertRecords(collection string, records *dal.RecordSet) error {
	return fmt.Errorf("NI")
}

func (self *BoltBackend) GetRecordById(collection string, id dal.Identity) (*dal.Record, error) {
	return nil, fmt.Errorf("NI")
}

func (self *BoltBackend) UpdateRecords(collection string, records *dal.RecordSet) error {
	return fmt.Errorf("NI")
}

func (self *BoltBackend) DeleteRecords(collection string, id []dal.Identity) error {
	return fmt.Errorf("NI")
}

func (self *BoltBackend) Query(collection string, filter filter.Filter) (*dal.RecordSet, error) {
	return nil, fmt.Errorf("NI")
}

func (self *BoltBackend) CreateCollection(definition dal.Collection) error {
	return fmt.Errorf("NI")
}

func (self *BoltBackend) DeleteCollection(collection string) error {
	return fmt.Errorf("NI")
}

func (self *BoltBackend) GetCollection(collection string) (dal.Collection, error) {
	return dal.Collection{}, fmt.Errorf("NI")
}
