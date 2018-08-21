package backends

import (
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type WrappedIndexer struct {
	RecordFunc func(*dal.Collection, *dal.Record) (*dal.Record, error)
	indexer    Indexer
	parent     Backend
}

func NewWrappedIndexer(indexer Indexer, parent Backend) *WrappedIndexer {
	return &WrappedIndexer{
		indexer: indexer,
		parent:  parent,
	}
}

func (self *WrappedIndexer) GetBackend() Backend {
	return self.parent
}

// passthrough the remaining functions to fulfill the Indexer interface
// -------------------------------------------------------------------------------------------------

func (self *WrappedIndexer) IndexConnectionString() *dal.ConnectionString {
	return self.indexer.IndexConnectionString()
}

func (self *WrappedIndexer) IndexInitialize(b Backend) error {
	return self.indexer.IndexInitialize(b)
}

func (self *WrappedIndexer) IndexExists(collection *dal.Collection, id interface{}) bool {
	return self.indexer.IndexExists(collection, id)
}

func (self *WrappedIndexer) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	record, err := self.indexer.IndexRetrieve(collection, id)

	if fn := self.RecordFunc; record != nil && fn != nil {
		record, err = fn(collection, record)
	}

	return record, err
}

func (self *WrappedIndexer) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return self.indexer.IndexRemove(collection, ids)
}

func (self *WrappedIndexer) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return self.indexer.Index(collection, records)
}

func (self *WrappedIndexer) QueryFunc(collection *dal.Collection, filter *filter.Filter, resultFn IndexResultFunc) error {
	if actualResultFn := resultFn; actualResultFn != nil && self.RecordFunc != nil {
		resultFn = self.resultFnFor(collection, actualResultFn)
	}

	return self.indexer.QueryFunc(collection, filter, resultFn)
}

func (self *WrappedIndexer) Query(collection *dal.Collection, filter *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	if self.RecordFunc != nil {
		for i, fn := range resultFns {
			resultFns[i] = self.resultFnFor(collection, fn)
		}
	}

	recordset, err := self.indexer.Query(collection, filter, resultFns...)

	if fn := self.RecordFunc; fn != nil && recordset != nil {
		for i, record := range recordset.Records {
			if r, err := fn(collection, record); err == nil {
				recordset.Records[i] = r
			} else {
				return recordset, err
			}
		}
	}

	return recordset, err
}

func (self *WrappedIndexer) ListValues(collection *dal.Collection, fields []string, filter *filter.Filter) (map[string][]interface{}, error) {
	return self.indexer.ListValues(collection, fields, filter)
}

func (self *WrappedIndexer) DeleteQuery(collection *dal.Collection, f *filter.Filter) error {
	return self.indexer.DeleteQuery(collection, f)
}

func (self *WrappedIndexer) FlushIndex() error {
	return self.indexer.FlushIndex()
}

func (self *WrappedIndexer) resultFnFor(collection *dal.Collection, actualResultFn IndexResultFunc) IndexResultFunc {
	return func(record *dal.Record, err error, page IndexPage) error {
		if err == nil {
			if r, err := self.RecordFunc(collection, record); err == nil {
				record = r
			} else {
				return err
			}
		}

		return actualResultFn(record, err, page)
	}
}
