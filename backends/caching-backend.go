package backends

import (
	"sync"
	"time"

	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type CachingBackend struct {
	backend Backend
	cache   sync.Map
}

func NewCachingBackend(parent Backend) *CachingBackend {
	return &CachingBackend{
		backend: parent,
	}
}

func (self *CachingBackend) ResetCache() {
	self.cache = sync.Map{}
}

func (self *CachingBackend) Retrieve(collection string, id interface{}, fields ...string) (*dal.Record, error) {
	cacheset := make(map[interface{}]interface{})

	if c, ok := self.cache.LoadOrStore(collection, cacheset); ok {
		cacheset = c.(map[interface{}]interface{})
	}

	if typeutil.IsScalar(id) {
		if recordI, ok := cacheset[id]; ok {
			return recordI.(*dal.Record), nil
		}
	}

	if record, err := self.backend.Retrieve(collection, id); err == nil {
		cacheset[id] = record
		return record, nil
	} else {
		return nil, err
	}
}

// passthrough the remaining functions to fulfill the Backend interface
// -------------------------------------------------------------------------------------------------
func (self *CachingBackend) Exists(collection string, id interface{}) bool {
	return self.backend.Exists(collection, id)
}

func (self *CachingBackend) Initialize() error {
	return self.backend.Initialize()
}

func (self *CachingBackend) SetIndexer(cs dal.ConnectionString) error {
	return self.backend.SetIndexer(cs)
}

func (self *CachingBackend) RegisterCollection(c *dal.Collection) {
	self.backend.RegisterCollection(c)
}

func (self *CachingBackend) GetConnectionString() *dal.ConnectionString {
	return self.backend.GetConnectionString()
}

func (self *CachingBackend) Insert(collection string, records *dal.RecordSet) error {
	return self.backend.Insert(collection, records)
}

func (self *CachingBackend) Update(collection string, records *dal.RecordSet, target ...string) error {
	return self.backend.Update(collection, records, target...)
}

func (self *CachingBackend) Delete(collection string, ids ...interface{}) error {
	return self.backend.Delete(collection, ids...)
}

func (self *CachingBackend) CreateCollection(definition *dal.Collection) error {
	return self.backend.CreateCollection(definition)
}

func (self *CachingBackend) DeleteCollection(collection string) error {
	return self.backend.DeleteCollection(collection)
}

func (self *CachingBackend) ListCollections() ([]string, error) {
	return self.backend.ListCollections()
}

func (self *CachingBackend) GetCollection(collection string) (*dal.Collection, error) {
	return self.backend.GetCollection(collection)
}

func (self *CachingBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	return self.backend.WithSearch(collection, filters...)
}

func (self *CachingBackend) WithAggregator(collection *dal.Collection) Aggregator {
	return self.backend.WithAggregator(collection)
}

func (self *CachingBackend) Flush() error {
	return self.backend.Flush()
}

func (self *CachingBackend) Ping(d time.Duration) error {
	return self.backend.Ping(d)
}
