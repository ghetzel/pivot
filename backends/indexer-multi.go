package backends

import (
	"fmt"
	"math/rand"

	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type IndexSelectionStrategy int

const (
	Sequential IndexSelectionStrategy = iota
	All
	First
	AllExceptFirst
	Random
)

func (self IndexSelectionStrategy) IsCompoundable() bool {
	switch self {
	case All, AllExceptFirst:
		return true
	}

	return false
}

type IndexOperation int

const (
	RetrieveOperation IndexOperation = iota
	PersistOperation
	DeleteOperation
	InspectionOperation
)

type MultiIndex struct {
	RetrievalStrategy  IndexSelectionStrategy
	PersistStrategy    IndexSelectionStrategy
	DeleteStrategy     IndexSelectionStrategy
	InspectionStrategy IndexSelectionStrategy
	indexers           []Indexer
	connectionStrings  []string
	backend            Backend
}

type IndexerResult struct {
	Index   int
	Indexer Indexer
}

type IndexerResultFunc func(indexer Indexer, current int, last int) error // {}
var IndexerResultsStop = fmt.Errorf(`stop indexer results`)

func NewMultiIndex(connectionStrings ...string) Indexer {
	return &MultiIndex{
		RetrievalStrategy:  Sequential,
		PersistStrategy:    All,
		DeleteStrategy:     All,
		InspectionStrategy: All,
		connectionStrings:  connectionStrings,
		indexers:           make([]Indexer, 0),
	}
}

func (self *MultiIndex) AddIndexer(indexer Indexer) error {
	// if our local IndexInitialize has already run, get this new indexer initialized
	if self.backend != nil {
		if err := indexer.IndexInitialize(self.backend); err != nil {
			return err
		}
	}

	self.indexers = append(self.indexers, indexer)
	return nil
}

func (self *MultiIndex) AddIndexerByConnectionString(cs string) error {
	if ics, err := dal.ParseConnectionString(cs); err == nil {
		if indexer, err := MakeIndexer(ics); err == nil {
			// if our local IndexInitialize has already run, get this new indexer initialized
			if self.backend != nil {
				if err := indexer.IndexInitialize(self.backend); err != nil {
					return err
				}
			}

			self.indexers = append(self.indexers, indexer)
		} else {
			return err
		}
	} else {
		return err
	}

	return nil
}

func (self *MultiIndex) IndexConnectionString() *dal.ConnectionString {
	if cs, err := dal.MakeConnectionString(`multi-index`, ``, ``, nil); err == nil {
		return &cs
	} else {
		return &dal.ConnectionString{}
	}
}

func (self *MultiIndex) IndexInitialize(backend Backend) error {
	self.backend = backend

	for _, cs := range self.connectionStrings {
		if err := self.AddIndexerByConnectionString(cs); err != nil {
			return err
		}
	}

	for _, indexer := range self.indexers {
		if err := indexer.IndexInitialize(self.backend); err != nil {
			return err
		}
	}

	return nil
}

func (self *MultiIndex) IndexExists(collection string, id interface{}) bool {
	exists := false

	if err := self.EachSelectedIndex(collection, InspectionOperation, func(indexer Indexer, _ int, _ int) error {
		if !indexer.IndexExists(collection, id) {
			exists = false
			log.Debugf("MultiIndex: Indexer %v/%v does not exist", indexer, collection)
			return IndexerResultsStop
		} else {
			exists = true
			return nil
		}
	}); err != nil {
		return false
	}

	return exists
}

func (self *MultiIndex) IndexRetrieve(collection string, id interface{}) (*dal.Record, error) {
	var record *dal.Record

	if err := self.EachSelectedIndex(collection, RetrieveOperation, func(indexer Indexer, _ int, _ int) error {
		if r, err := indexer.IndexRetrieve(collection, id); err == nil {
			record = r
			return IndexerResultsStop
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if record == nil {
		return nil, fmt.Errorf("Index document %v/%v does not exist", collection, id)
	}

	return record, nil
}

func (self *MultiIndex) IndexRemove(collection string, ids []interface{}) error {
	var indexErr error

	if err := self.EachSelectedIndex(collection, DeleteOperation, func(indexer Indexer, _ int, _ int) error {
		if err := indexer.IndexRemove(collection, ids); err != nil {
			log.Debugf("MultiIndex: Failed to remove IDs from %v from indexer %v: %v", collection, indexer, err)
			indexErr = err
		}

		return nil
	}); err != nil {
		return err
	}

	return indexErr
}

func (self *MultiIndex) Index(collection string, records *dal.RecordSet) error {
	var indexErr error

	if err := self.EachSelectedIndex(collection, PersistOperation, func(indexer Indexer, _ int, _ int) error {
		if err := indexer.Index(collection, records); err != nil {
			log.Debugf("MultiIndex: Failed to persist records in indexer %v: %v", indexer, err)
			indexErr = err
		}

		return nil
	}); err != nil {
		return err
	}

	return indexErr
}

func (self *MultiIndex) QueryFunc(collection string, filter filter.Filter, resultFn IndexResultFunc) error {
	var indexErr error

	if err := self.EachSelectedIndex(collection, RetrieveOperation, func(indexer Indexer, _ int, _ int) error {
		if err := indexer.QueryFunc(collection, filter, resultFn); err == nil {
			if self.RetrievalStrategy.IsCompoundable() {
				return IndexerResultsStop
			}
		} else {
			indexErr = err
			log.Debugf("MultiIndex: Indexer query to %v/%v failed: %v", indexer, collection, err)
		}

		return nil
	}); err != nil {
		return err
	}

	return indexErr
}

func (self *MultiIndex) Query(collection string, filter filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	recordset := dal.NewRecordSet()
	var indexErr error

	if err := self.EachSelectedIndex(collection, RetrieveOperation, func(indexer Indexer, _ int, _ int) error {
		if rs, err := indexer.Query(collection, filter, resultFns...); err == nil {
			if !rs.IsEmpty() {
				if self.RetrievalStrategy.IsCompoundable() {
					recordset.Append(rs)
				} else {
					recordset = rs
					return IndexerResultsStop
				}
			}
		} else {
			indexErr = err
			log.Debugf("MultiIndex: Indexer query to %v/%v failed: %v", indexer, collection, err)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return recordset, indexErr
}

func (self *MultiIndex) ListValues(collection string, fields []string, filter filter.Filter) (map[string][]interface{}, error) {
	values := make(map[string][]interface{})
	var indexErr error

	if err := self.EachSelectedIndex(collection, RetrieveOperation, func(indexer Indexer, _ int, _ int) error {
		if kv, err := indexer.ListValues(collection, fields, filter); err == nil {
			if len(kv) > 0 {
				if self.RetrievalStrategy.IsCompoundable() {
					for k, v := range kv {
						if vv, ok := values[k]; ok {
							values[k] = append(vv, v...)
						} else {
							values[k] = v
						}
					}
				} else {
					values = kv
					return IndexerResultsStop
				}
			}
		} else {
			indexErr = err
			log.Debugf("MultiIndex: Indexer list values %v/%v failed: %v", indexer, collection, err)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return values, indexErr
}

func (self *MultiIndex) DeleteQuery(collection string, f filter.Filter) error {
	var indexErr error

	if err := self.EachSelectedIndex(collection, DeleteOperation, func(indexer Indexer, _ int, _ int) error {
		if err := indexer.DeleteQuery(collection, f); err != nil {
			log.Debugf("MultiIndex: Failed to remove by query %v from %v, %v: %v", f, collection, indexer, err)
			indexErr = err
		}

		return nil
	}); err != nil {
		return err
	}

	return indexErr
}

func (self *MultiIndex) EachSelectedIndex(collection string, operation IndexOperation, resultFn IndexerResultFunc) error {
	lastIndexer := -1

	for {
		if results, err := self.SelectIndex(collection, operation, lastIndexer); err == nil {
			for _, result := range results {
				if err := resultFn(result.Indexer, result.Index, lastIndexer); err != nil {
					if err == IndexerResultsStop {
						return nil
					} else {
						return err
					}
				}

				lastIndexer = result.Index
			}
		} else if results == nil {
			break
		} else {
			return err
		}
	}

	return nil
}

func (self *MultiIndex) SelectIndex(collection string, operation IndexOperation, lastIndexer int) ([]IndexerResult, error) {
	var strategy IndexSelectionStrategy

	switch operation {
	case RetrieveOperation:
		strategy = self.RetrievalStrategy
	case PersistOperation:
		strategy = self.PersistStrategy
	case DeleteOperation:
		strategy = self.DeleteStrategy
	case InspectionOperation:
		strategy = self.InspectionStrategy
	default:
		return nil, fmt.Errorf("Unrecognized index operation '%v'", operation)
	}

	if len(self.indexers) == 0 {
		return nil, fmt.Errorf("No indexers registered")
	}

	switch strategy {
	case Sequential:
		if i := (lastIndexer + 1); i < len(self.indexers) {
			return []IndexerResult{
				{
					Index:   i,
					Indexer: self.indexers[i],
				},
			}, nil
		} else {
			return nil, nil
		}

	case First:
		return []IndexerResult{
			{
				Index:   0,
				Indexer: self.indexers[0],
			},
		}, nil

	case AllExceptFirst:
		rv := make([]IndexerResult, 0)

		for i, indexer := range self.indexers[1:] {
			rv = append(rv, IndexerResult{
				Index:   (i + 1),
				Indexer: indexer,
			})
		}

		return rv, nil

	case All:
		rv := make([]IndexerResult, 0)

		for i, indexer := range self.indexers {
			rv = append(rv, IndexerResult{
				Index:   i,
				Indexer: indexer,
			})
		}

		return rv, nil

	case Random:
		i := rand.Intn(len(self.indexers))

		return []IndexerResult{
			{
				Index:   i,
				Indexer: self.indexers[i],
			},
		}, nil

	default:
		return nil, fmt.Errorf("Unrecognized selection strategy '%v'", strategy)
	}
}
