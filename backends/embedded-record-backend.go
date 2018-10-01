package backends

import (
	"sync"
	"time"

	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type EmbeddedRecordBackend struct {
	backend Backend
	cache   sync.Map
}

func NewEmbeddedRecordBackend(parent Backend) *EmbeddedRecordBackend {
	return &EmbeddedRecordBackend{
		backend: parent,
	}
}

func (self *EmbeddedRecordBackend) Inflate(collection *dal.Collection, record *dal.Record) (*dal.Record, error) {
	if collection != nil {
		if err := InflateEmbeddedRecords(self, collection, record, nil); err == nil {
			return record, nil
		} else {
			return nil, err
		}
	} else {
		return record, nil
	}
}

func (self *EmbeddedRecordBackend) String() string {
	return self.backend.String()
}

func (self *EmbeddedRecordBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if record, err := self.backend.Retrieve(name, id, fields...); err == nil {
			return self.Inflate(collection, record)
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *EmbeddedRecordBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	if indexer := self.backend.WithSearch(collection, filters...); indexer != nil {
		wi := NewWrappedIndexer(indexer, self)
		wi.RecordFunc = self.Inflate

		return wi
	} else {
		return nil
	}
}

// passthrough the remaining functions to fulfill the Backend interface
// -------------------------------------------------------------------------------------------------
func (self *EmbeddedRecordBackend) Exists(collection string, id interface{}) bool {
	return self.backend.Exists(collection, id)
}

func (self *EmbeddedRecordBackend) Initialize() error {
	return self.backend.Initialize()
}

func (self *EmbeddedRecordBackend) SetIndexer(cs dal.ConnectionString) error {
	return self.backend.SetIndexer(cs)
}

func (self *EmbeddedRecordBackend) RegisterCollection(c *dal.Collection) {
	self.backend.RegisterCollection(c)
}

func (self *EmbeddedRecordBackend) GetConnectionString() *dal.ConnectionString {
	return self.backend.GetConnectionString()
}

func (self *EmbeddedRecordBackend) Insert(collection string, records *dal.RecordSet) error {
	return self.backend.Insert(collection, records)
}

func (self *EmbeddedRecordBackend) Update(collection string, records *dal.RecordSet, target ...string) error {
	return self.backend.Update(collection, records, target...)
}

func (self *EmbeddedRecordBackend) Delete(collection string, ids ...interface{}) error {
	return self.backend.Delete(collection, ids...)
}

func (self *EmbeddedRecordBackend) CreateCollection(definition *dal.Collection) error {
	return self.backend.CreateCollection(definition)
}

func (self *EmbeddedRecordBackend) DeleteCollection(collection string) error {
	return self.backend.DeleteCollection(collection)
}

func (self *EmbeddedRecordBackend) ListCollections() ([]string, error) {
	return self.backend.ListCollections()
}

func (self *EmbeddedRecordBackend) GetCollection(collection string) (*dal.Collection, error) {
	return self.backend.GetCollection(collection)
}

func (self *EmbeddedRecordBackend) WithAggregator(collection *dal.Collection) Aggregator {
	return self.backend.WithAggregator(collection)
}

func (self *EmbeddedRecordBackend) Flush() error {
	return self.backend.Flush()
}

func (self *EmbeddedRecordBackend) Ping(d time.Duration) error {
	return self.backend.Ping(d)
}

func (self *EmbeddedRecordBackend) Supports(feature ...BackendFeature) bool {
	return self.backend.Supports(feature...)
}
