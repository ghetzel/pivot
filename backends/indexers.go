package backends

import (
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type IndexPage struct {
	Page         int
	TotalPages   int
	PageSize     int
	Offset       int
	TotalResults uint64
}

type IndexResultFunc func(record *dal.Record, page IndexPage) error // {}

type Indexer interface {
	Initialize(Backend) error
	Exists(collection string, id string) bool
	Retrieve(collection string, id string) (*dal.Record, error)
	Index(collection string, records *dal.RecordSet) error
	QueryFunc(collection string, filter filter.Filter, resultFn IndexResultFunc) error
	Query(collection string, filter filter.Filter) (*dal.RecordSet, error)
	QueryString(collection string, filterString string) (*dal.RecordSet, error)
	Remove(collection string, ids []string) error
	ListValues(collection string, fields []string, filter filter.Filter) (*dal.RecordSet, error)
}

func DefaultQueryString(indexer Indexer, collection string, filterString string) (*dal.RecordSet, error) {
	if f, err := filter.Parse(filterString); err == nil {
		return indexer.Query(collection, f)
	} else {
		return nil, err
	}
}

func MakeIndexer(connection dal.ConnectionString) (Indexer, error) {
	log.Debugf("Creating indexer for connection string %q", connection.String())

	switch connection.Backend() {
	case `bleve`:
		return NewBleveIndexer(connection), nil
	default:
		return nil, fmt.Errorf("Unknown indexer type %q", connection.Backend())
	}
}
