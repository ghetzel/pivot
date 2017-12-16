package backends

import (
	"fmt"
	"math"

	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
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

type IndexResultFunc func(record *dal.Record, err error, page IndexPage) error // {}

type Indexer interface {
	IndexConnectionString() *dal.ConnectionString
	IndexInitialize(Backend) error
	IndexExists(index string, id interface{}) bool
	IndexRetrieve(index string, id interface{}) (*dal.Record, error)
	IndexRemove(index string, ids []interface{}) error
	Index(index string, records *dal.RecordSet) error
	QueryFunc(index string, filter *filter.Filter, resultFn IndexResultFunc) error
	Query(index string, filter *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error)
	ListValues(index string, fields []string, filter *filter.Filter) (map[string][]interface{}, error)
	DeleteQuery(index string, f *filter.Filter) error
	FlushIndex() error
}

func MakeIndexer(connection dal.ConnectionString) (Indexer, error) {
	log.Infof("Creating indexer: %v", connection.String())

	switch connection.Backend() {
	case `bleve`:
		return NewBleveIndexer(connection), nil
	case `elasticsearch`:
		return NewElasticsearchIndexer(connection), nil
	default:
		return nil, fmt.Errorf("Unknown indexer type %q", connection.Backend())
	}
}

func PopulateRecordSetPageDetails(recordset *dal.RecordSet, f *filter.Filter, page IndexPage) {
	// result count is whatever we were told it was for this query
	if page.TotalResults >= 0 {
		recordset.KnownSize = true
		recordset.ResultCount = page.TotalResults
	} else {
		recordset.ResultCount = int64(len(recordset.Records))
	}

	if page.TotalPages > 0 {
		recordset.TotalPages = page.TotalPages
	} else if recordset.ResultCount >= 0 && f.Limit > 0 {
		// total pages = ceil(result count / page size)
		recordset.TotalPages = int(math.Ceil(float64(recordset.ResultCount) / float64(f.Limit)))
	} else {
		recordset.TotalPages = 1
	}

	if recordset.RecordsPerPage == 0 {
		recordset.RecordsPerPage = page.Limit
	}

	// page is the last page number set
	if page.Limit > 0 {
		recordset.Page = int(math.Ceil(float64(f.Offset+1) / float64(page.Limit)))
	}
}

func DefaultQueryImplementation(indexer Indexer, collection string, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	recordset := dal.NewRecordSet()

	if err := indexer.QueryFunc(collection, f, func(record *dal.Record, err error, page IndexPage) error {
		defer PopulateRecordSetPageDetails(recordset, f, page)

		if len(resultFns) > 0 {
			resultFn := resultFns[0]

			if f.IdOnly() {
				return resultFn(dal.NewRecord(record.ID), err, page)
			} else {
				return resultFn(record, err, page)
			}
		} else {
			if f.IdOnly() {
				recordset.Records = append(recordset.Records, dal.NewRecord(record.ID))
			} else {
				recordset.Records = append(recordset.Records, record)
			}

			return nil
		}
	}); err != nil {
		return nil, err
	}

	return recordset, nil
}
