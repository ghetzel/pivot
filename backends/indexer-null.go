package backends

import (
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type NullIndexer struct {
}

func (self *NullIndexer) IndexConnectionString() *dal.ConnectionString {
	return nil
}

func (self *NullIndexer) IndexInitialize(Backend) error {
	return NotImplementedError
}

func (self *NullIndexer) IndexExists(collection string, id interface{}) bool {
	return false
}

func (self *NullIndexer) IndexRetrieve(collection string, id interface{}) (*dal.Record, error) {
	return nil, NotImplementedError
}

func (self *NullIndexer) IndexRemove(collection string, ids []interface{}) error {
	return NotImplementedError
}

func (self *NullIndexer) Index(collection string, records *dal.RecordSet) error {
	return NotImplementedError
}

func (self *NullIndexer) QueryFunc(collection string, filter filter.Filter, resultFn IndexResultFunc) error {
	return NotImplementedError
}

func (self *NullIndexer) Query(collection string, filter filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	return nil, NotImplementedError
}

func (self *NullIndexer) ListValues(collection string, fields []string, filter filter.Filter) (map[string][]interface{}, error) {
	return nil, NotImplementedError
}

func (self *NullIndexer) DeleteQuery(collection string, f filter.Filter) error {
	return NotImplementedError
}

func (self *NullIndexer) FlushIndex() error {
	return NotImplementedError
}
