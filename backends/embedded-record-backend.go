package backends

import (
	"sync"
	"time"

	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type EmbeddedRecordBackend struct {
	SkipKeys []string
	backend  Backend
	indexer  Indexer
	cache    sync.Map
}

func NewEmbeddedRecordBackend(parent Backend, skipKeys ...string) *EmbeddedRecordBackend {
	backend := &EmbeddedRecordBackend{
		SkipKeys: skipKeys,
		backend:  parent,
	}

	if indexer := parent.WithSearch(nil); indexer != nil {
		backend.indexer = indexer
	}

	return backend
}

func (self *EmbeddedRecordBackend) EmbedRelationships(collection *dal.Collection, record *dal.Record, fields ...string) (*dal.Record, error) {
	if collection != nil {
		if err := PopulateRelationships(self, collection, record, nil, fields...); err == nil {
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
			record, err = self.EmbedRelationships(collection, record, fields...)
			ResolveDeferredRecords(nil, record)
			return record, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *EmbeddedRecordBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	if self.indexer != nil {
		return self
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

// fulfill the Indexer interface
// -------------------------------------------------------------------------------------------------
func (self *EmbeddedRecordBackend) GetBackend() Backend {
	return self
}

func (self *EmbeddedRecordBackend) IndexConnectionString() *dal.ConnectionString {
	return self.indexer.IndexConnectionString()
}

func (self *EmbeddedRecordBackend) IndexInitialize(b Backend) error {
	return self.indexer.IndexInitialize(b)
}

func (self *EmbeddedRecordBackend) IndexExists(collection *dal.Collection, id interface{}) bool {
	return self.indexer.IndexExists(collection, id)
}

func (self *EmbeddedRecordBackend) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	return self.indexer.IndexRetrieve(collection, id)
}

func (self *EmbeddedRecordBackend) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return self.indexer.IndexRemove(collection, ids)
}

func (self *EmbeddedRecordBackend) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return self.indexer.Index(collection, records)
}

func (self *EmbeddedRecordBackend) QueryFunc(collection *dal.Collection, filter *filter.Filter, resultFn IndexResultFunc) error {
	deferredCache := make(map[string]interface{})

	return self.indexer.QueryFunc(collection, filter, func(record *dal.Record, err error, page IndexPage) error {
		if err := ResolveDeferredRecords(deferredCache, record); err == nil {
			return resultFn(record, err, page)
		} else {
			return err
		}
	})
}

func (self *EmbeddedRecordBackend) Query(collection *dal.Collection, filter *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	recordset, err := self.indexer.Query(collection, filter, resultFns...)

	for i, record := range recordset.Records {
		if record, err := self.EmbedRelationships(collection, record, filter.Fields...); err == nil {
			recordset.Records[i] = record
		} else {
			return nil, err
		}
	}

	deferredCache := make(map[string]interface{})

	if err := ResolveDeferredRecords(deferredCache, recordset.Records...); err != nil {
		return nil, err
	}

	return recordset, err
}

func (self *EmbeddedRecordBackend) ListValues(collection *dal.Collection, fields []string, filter *filter.Filter) (map[string][]interface{}, error) {
	return self.indexer.ListValues(collection, fields, filter)
}

func (self *EmbeddedRecordBackend) DeleteQuery(collection *dal.Collection, f *filter.Filter) error {
	return self.indexer.DeleteQuery(collection, f)
}

func (self *EmbeddedRecordBackend) FlushIndex() error {
	return self.indexer.FlushIndex()
}
