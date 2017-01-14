package backends

import (
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"math"
)

var IndexerPageSize int = 100
var MaxFacetCardinality int = 10000

type IndexPage struct {
	Page         int
	TotalPages   int
	Limit        int
	Offset       int
	TotalResults int64
}

type IndexResultFunc func(record *dal.Record, page IndexPage) error // {}

type Indexer interface {
	IndexConnectionString() *dal.ConnectionString
	IndexInitialize(Backend) error
	IndexExists(collection string, id interface{}) bool
	IndexRetrieve(collection string, id interface{}) (*dal.Record, error)
	IndexRemove(collection string, ids []interface{}) error
	Index(collection string, records *dal.RecordSet) error
	QueryFunc(collection string, filter filter.Filter, resultFn IndexResultFunc) error
	Query(collection string, filter filter.Filter) (*dal.RecordSet, error)
	ListValues(collection string, fields []string, filter filter.Filter) (map[string][]interface{}, error)
	DeleteQuery(collection string, f filter.Filter) error
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

func PopulateRecordSetPageDetails(recordset *dal.RecordSet, f filter.Filter, page IndexPage) {
	// result count is whatever we were told it was for this query
	recordset.ResultCount = page.TotalResults

	if page.TotalPages > 0 {
		recordset.TotalPages = page.TotalPages
	} else if recordset.ResultCount >= 0 && f.Limit > 0 {
		// total pages = ceil(result count / page size)
		recordset.TotalPages = int(math.Ceil(float64(recordset.ResultCount) / float64(f.Limit)))
	}

	if recordset.RecordsPerPage == 0 {
		recordset.RecordsPerPage = page.Limit
	}

	// page is the last page number set
	if page.Limit > 0 {
		recordset.Page = int(math.Ceil(float64(f.Offset+1) / float64(page.Limit)))
	}
}
