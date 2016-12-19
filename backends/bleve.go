package backends

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"path"
	"strings"
	"math"
)

var BleveIndexerPageSize int = 1000

type BleveIndexer struct {
	Indexer
	conn       dal.ConnectionString
	parent     Backend
	indexCache map[string]bleve.Index
}

func NewBleveIndexer(connection dal.ConnectionString) *BleveIndexer {
	return &BleveIndexer{
		conn:       connection,
		indexCache: make(map[string]bleve.Index),
	}
}

func (self *BleveIndexer) Initialize(parent Backend) error {
	self.parent = parent

	return nil
}

func (self *BleveIndexer) Retrieve(collection string, id string) (*dal.Record, error) {
	if index, err := self.getIndexForCollection(collection); err == nil {
		request := bleve.NewSearchRequest(bleve.NewDocIDQuery([]string{id}))

		if results, err := index.Search(request); err == nil {
			if results.Total == 1 {
				return dal.NewRecord(results.Hits[0].ID).SetFields(results.Hits[0].Fields), nil
			} else {
				return nil, fmt.Errorf("Too many results; expected: 1, got: %d", results.Total)
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *BleveIndexer) Exists(collection string, id string) bool {
	if _, err := self.Retrieve(collection, id); err == nil {
		return true
	}

	return false
}

func (self *BleveIndexer) Index(collection string, records *dal.RecordSet) error {
	if index, err := self.getIndexForCollection(collection); err == nil {
		batch := index.NewBatch()

		for _, record := range records.Records {
			if err := batch.Index(string(record.ID), record.Fields); err != nil {
				return err
			}
		}

		return index.Batch(batch)
	} else {
		return err
	}
}

func (self *BleveIndexer) QueryFunc(collection string, f filter.Filter, resultFn IndexResultFunc) error {
	if index, err := self.getIndexForCollection(collection); err == nil {
		if bq, err := self.filterToBleveQuery(index, f); err == nil {
			offset := 0
			page := 1

			for {
				request := bleve.NewSearchRequestOptions(bq, BleveIndexerPageSize, offset, false)

				if results, err := index.Search(request); err == nil {
					totalPages := int(math.Ceil(float64(results.Total) / float64(BleveIndexerPageSize)))

					// call the resultFn for each hit on this page
					for _, hit := range results.Hits {
						if err := resultFn(dal.NewRecord(hit.ID).SetFields(hit.Fields), IndexPage{
							Page:         page,
							TotalPages:   totalPages,
							PerPage: BleveIndexerPageSize,
							Offset:       offset,
							TotalResults: results.Total,
						}); err != nil {
							return err
						}
					}

					// increment offset by the page size we just processed
					offset += len(results.Hits)
					page += 1

					// if the offset is now beyond the total results count
					if uint64(offset) >= results.Total {
						return nil
					}
				} else {
					return err
				}
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *BleveIndexer) Query(collection string, f filter.Filter) (*dal.RecordSet, error) {
	recordset := dal.NewRecordSet()

	if err := self.QueryFunc(collection, f, func(indexRecord *dal.Record, page IndexPage) error {
		if recordset.TotalPages == 0 {
			recordset.TotalPages = page.TotalPages
		}

		if recordset.RecordsPerPage == 0 {
			recordset.RecordsPerPage = page.PerPage
		}

		recordset.Page = page.Page

		if record, err := self.parent.Retrieve(collection, indexRecord.ID); err == nil {
			recordset.Push(record)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return recordset, nil
}

func (self *BleveIndexer) QueryString(collection string, filterString string) (*dal.RecordSet, error) {
	return DefaultQueryString(self, collection, filterString)
}

func (self *BleveIndexer) Remove(collection string, ids []string) error {
	if index, err := self.getIndexForCollection(collection); err == nil {
		batch := index.NewBatch()

		for _, id := range ids {
			batch.Delete(string(id))
		}

		return index.Batch(batch)
	} else {
		return err
	}
}

func (self *BleveIndexer) getIndexForCollection(collection string) (bleve.Index, error) {
	if v, ok := self.indexCache[collection]; ok {
		return v, nil
	} else {
		var index bleve.Index
		mapping := bleve.NewIndexMapping()

		switch self.conn.Dataset() {
		case `/memory`:
			if ix, err := bleve.NewMemOnly(mapping); err == nil {
				index = ix
			} else {
				return nil, err
			}
		default:
			indexBaseDir := self.conn.Dataset()
			indexPath := path.Join(indexBaseDir, collection)

			if ix, err := bleve.Open(indexPath); err == nil {
				index = ix
			} else if ix, err := bleve.New(indexPath, mapping); err == nil {
				index = ix
			} else {
				return nil, err
			}
		}

		self.indexCache[collection] = index
		return index, nil
	}
}

func (self *BleveIndexer) filterToBleveQuery(index bleve.Index, f filter.Filter) (query.Query, error) {
	if f.MatchAll {
		return bleve.NewMatchAllQuery(), nil
	} else {
		conjunction := bleve.NewConjunctionQuery()

		for _, criterion := range f.Criteria {
			var skipNext bool
			var disjunction *query.DisjunctionQuery

			// this handles AND (field=a OR b OR ...)
			if len(criterion.Values) > 1 {
				disjunction = bleve.NewDisjunctionQuery()
			}

			for _, value := range criterion.Values {
				// objects are indexed case-insensitive, so queries should be too
				value = strings.ToLower(value)

				var currentQuery query.FieldableQuery

				switch criterion.Operator {
				case `is`, ``:
					if criterion.Field == `_id` {
						conjunction.AddQuery(bleve.NewDocIDQuery(criterion.Values))
						skipNext = true
						break
					} else {
						if value == `null` {
							currentQuery = bleve.NewTermQuery(``)
						} else {
							currentQuery = bleve.NewTermQuery(value)
						}
					}
				case `prefix`:
					currentQuery = bleve.NewWildcardQuery(value + `*`)
				case `suffix`:
					currentQuery = bleve.NewWildcardQuery(`*` + value)
				case `contains`:
					currentQuery = bleve.NewWildcardQuery(`*` + value + `*`)

				case `gt`, `lt`, `gte`, `lte`:
					var min, max *float64
					var minInc, maxInc bool

					if v, err := stringutil.ConvertToFloat(value); err == nil {
						if strings.HasPrefix(criterion.Operator, `gt`) {
							min = &v
							minInc = strings.HasSuffix(criterion.Operator, `e`)
						} else {
							max = &v
							maxInc = strings.HasSuffix(criterion.Operator, `e`)
						}
					} else {
						return nil, err
					}

					currentQuery = bleve.NewNumericRangeInclusiveQuery(min, max, &minInc, &maxInc)

				// case `not`:
				// 	q := bleve.NewBooleanQuery()
				// 	var subquery query.FieldableQuery

				// 	if value == `null` {
				// 		subquery = bleve.NewTermQuery(``)
				// 	} else {
				// 		subquery = bleve.NewTermQuery(value)
				// 	}

				// 	subquery.SetField(criterion.Field)
				// 	q.AddMustNot(subquery)

				// 	if disjunction != nil {
				// 		disjunction.AddQuery(q)
				// 		conjunction.AddQuery(disjunction)
				// 	}else{
				// 		conjunction.AddQuery(q)
				// 	}

				// 	continue

				default:
					return nil, fmt.Errorf("Unimplemented operator '%s'", criterion.Operator)
				}

				if currentQuery != nil {
					currentQuery.SetField(criterion.Field)

					if disjunction != nil {
						disjunction.AddQuery(currentQuery)
					} else {
						conjunction.AddQuery(currentQuery)
					}
				}
			}

			if skipNext {
				continue
			}

			if disjunction != nil {
				conjunction.AddQuery(disjunction)
			}
		}

		if len(conjunction.Conjuncts) > 0 {
			return conjunction, nil
		} else {
			return nil, fmt.Errorf("Filter did not produce a valid query")
		}
	}
}
