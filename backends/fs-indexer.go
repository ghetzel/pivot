package backends

import (
	"fmt"

	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

func (self *FilesystemBackend) IndexConnectionString() *dal.ConnectionString {
	return &dal.ConnectionString{}
}

func (self *FilesystemBackend) IndexInitialize(backend Backend) error {
	return nil
}

func (self *FilesystemBackend) IndexExists(collection string, id interface{}) bool {
	return true
}

func (self *FilesystemBackend) IndexRetrieve(collection string, id interface{}) (*dal.Record, error) {
	return nil, nil
}

func (self *FilesystemBackend) IndexRemove(collection string, ids []interface{}) error {
	return fmt.Errorf(`Not Implemented`)
}

func (self *FilesystemBackend) Index(collection string, records *dal.RecordSet) error {
	return fmt.Errorf(`Not Implemented`)
}

func (self *FilesystemBackend) QueryFunc(collection string, filter filter.Filter, resultFn IndexResultFunc) error {
	return fmt.Errorf(`Not Implemented`)
}

func (self *FilesystemBackend) Query(collection string, filter filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	return nil, fmt.Errorf(`Not Implemented`)
}

func (self *FilesystemBackend) ListValues(collection string, fields []string, filter filter.Filter) (map[string][]interface{}, error) {
	return nil, fmt.Errorf(`Not Implemented`)
}

func (self *FilesystemBackend) DeleteQuery(collection string, f filter.Filter) error {
	return fmt.Errorf(`Not Implemented`)
}
