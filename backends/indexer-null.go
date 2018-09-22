package backends

import (
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type NullIndexer struct {
}

func (self *NullIndexer) IndexConnectionString() *dal.ConnectionString {
	return nil
}

func (self *NullIndexer) IndexInitialize(Backend) error {
	return NotImplementedError
}

func (self *NullIndexer) GetBackend() Backend {
	return nil
}

func (self *NullIndexer) IndexExists(collection *dal.Collection, id interface{}) bool {
	return false
}

func (self *NullIndexer) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	return nil, NotImplementedError
}

func (self *NullIndexer) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return NotImplementedError
}

func (self *NullIndexer) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return NotImplementedError
}

func (self *NullIndexer) QueryFunc(collection *dal.Collection, filter filter.Filter, resultFn IndexResultFunc) error {
	return NotImplementedError
}

func (self *NullIndexer) Query(collection *dal.Collection, filter filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	return nil, NotImplementedError
}

func (self *NullIndexer) ListValues(collection *dal.Collection, fields []string, filter filter.Filter) (map[string][]interface{}, error) {
	return nil, NotImplementedError
}

func (self *NullIndexer) DeleteQuery(collection *dal.Collection, f filter.Filter) error {
	return NotImplementedError
}

func (self *NullIndexer) FlushIndex() error {
	return NotImplementedError
}
