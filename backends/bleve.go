package backends

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/analysis/char/regexp"
	"github.com/blevesearch/bleve/analysis/token/lowercase"
	"github.com/blevesearch/bleve/analysis/tokenizer/single"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"math"
	"path"
	"strings"
)

var BleveIndexerPageSize int = 100
var BleveMaxFacetCardinality int = 10000

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
			offset := f.Offset
			page := 1
			processed := 0

			// filter size falls back to package default
			if f.Size == 0 {
				f.Size = BleveIndexerPageSize
			}

			// perform requests until we have enough results or the index is out of them
			for {
				request := bleve.NewSearchRequestOptions(bq, f.Size, offset, false)

				// apply sorting (if specified)
				if f.Sort != nil && len(f.Sort) > 0 {
					request.SortBy(f.Sort)
				}

				// apply restriction on returned fields
				if f.Fields != nil {
					request.Fields = f.Fields
				}

				// perform search
				if results, err := index.Search(request); err == nil {
					if len(results.Hits) == 0 {
						return nil
					}

					total := results.Total

					// if the specified limit is less than the total results, then total = limit
					if f.Limit > 0 && uint64(f.Limit) < total {
						total = uint64(f.Limit)
					}

					// totalPages = ceil(result count / page size)
					totalPages := int(math.Ceil(float64(total) / float64(f.Size)))

					// call the resultFn for each hit on this page
					for _, hit := range results.Hits {
						if err := resultFn(dal.NewRecord(hit.ID).SetFields(hit.Fields), IndexPage{
							Page:         page,
							TotalPages:   totalPages,
							PerPage:      f.Size,
							Offset:       offset,
							TotalResults: results.Total,
						}); err != nil {
							return err
						}

						processed += 1

						// if we have a limit set and we're at or beyond it
						if f.Limit > 0 && processed >= f.Limit {
							return nil
						}
					}

					// increment offset by the page size we just processed
					page += 1
					offset += len(results.Hits)

					// if the offset is now beyond the total results count
					if uint64(processed) >= total {
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

		// result count is whatever Bleve told us it was for this query
		recordset.ResultCount = page.TotalResults

		// page is the last page number set
		recordset.Page = page.Page

		if f.IdOnly() {
			recordset.Records = append(recordset.Records, dal.NewRecord(indexRecord.ID))
		} else {
			if record, err := self.parent.Retrieve(collection, indexRecord.ID); err == nil {
				recordset.Records = append(recordset.Records, record)
			}
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

func (self *BleveIndexer) ListValues(collection string, fields []string, f filter.Filter) (*dal.RecordSet, error) {
	if index, err := self.getIndexForCollection(collection); err == nil {
		if bq, err := self.filterToBleveQuery(index, f); err == nil {
			request := bleve.NewSearchRequestOptions(bq, 0, 0, false)
			request.Fields = []string{}
			idQuery := false

			for _, field := range fields {
				switch field {
				case `_id`, `id`:
					idQuery = true
					request.Size = BleveMaxFacetCardinality
					request.Fields = append(request.Fields, `_id`)
				default:
					request.AddFacet(
						field,
						bleve.NewFacetRequest(field, BleveMaxFacetCardinality),
					)
				}
			}

			if results, err := index.Search(request); err == nil {
				recordset := dal.NewRecordSet()
				groupedByField := make(map[string]*dal.Record)

				for name, facet := range results.Facets {
					for _, term := range facet.Terms {
						var record *dal.Record

						if r, ok := groupedByField[name]; ok {
							record = r
						} else {
							record = dal.NewRecord(name)
							groupedByField[name] = record
						}

						record.Append(`values`, term.Term)
					}
				}

				if idQuery {
					for _, hit := range results.Hits {
						var record *dal.Record

						if r, ok := groupedByField[`_id`]; ok {
							record = r
						} else {
							record = dal.NewRecord(`_id`)
							groupedByField[`_id`] = record
						}

						record.Append(`values`, hit.ID)
					}
				}

				for _, field := range fields {
					if record, ok := groupedByField[field]; ok {
						recordset.Push(record)
					} else {
						return nil, fmt.Errorf("Field %q missing from result set", field)
					}
				}

				return recordset, nil
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *BleveIndexer) getIndexForCollection(collection string) (bleve.Index, error) {
	if v, ok := self.indexCache[collection]; ok {
		return v, nil
	} else {
		var index bleve.Index
		mapping := bleve.NewIndexMapping()

		// setup the mapping and text analysis settings for this index
		self.useFilterMapping(mapping)

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
		mapping := index.Mapping()
		conjunction := bleve.NewConjunctionQuery()

		for _, criterion := range f.Criteria {
			var skipNext bool
			var disjunction *query.DisjunctionQuery

			analyzerName := mapping.AnalyzerNameForPath(criterion.Field)

			// this handles AND (field=a OR b OR ...)
			if len(criterion.Values) > 1 {
				disjunction = bleve.NewDisjunctionQuery()
			}

			for _, value := range criterion.Values {
				var analyzedValue string

				if az := mapping.AnalyzerNamed(analyzerName); az != nil {
					for _, token := range az.Analyze([]byte(value[:])) {
						analyzedValue += string(token.Term[:])
					}
				} else {
					analyzedValue = value
				}

				var currentQuery query.FieldableQuery

				switch criterion.Operator {
				case `is`, ``:
					if criterion.Field == `_id` {
						conjunction.AddQuery(bleve.NewDocIDQuery(criterion.Values))
						skipNext = true
						break
					} else {
						if analyzedValue == `null` {
							currentQuery = bleve.NewTermQuery(``)
						} else {
							currentQuery = bleve.NewTermQuery(analyzedValue)
						}
					}
				case `prefix`:
					currentQuery = bleve.NewWildcardQuery(analyzedValue + `*`)
				case `suffix`:
					currentQuery = bleve.NewWildcardQuery(`*` + analyzedValue)
				case `contains`:
					currentQuery = bleve.NewWildcardQuery(`*` + analyzedValue + `*`)

				case `gt`, `lt`, `gte`, `lte`:
					var min, max *float64
					var minInc, maxInc bool

					if v, err := stringutil.ConvertToFloat(analyzedValue); err == nil {
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

				// 	if analyzedValue == `null` {
				// 		subquery = bleve.NewTermQuery(``)
				// 	} else {
				// 		subquery = bleve.NewTermQuery(analyzedValue)
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

func (self *BleveIndexer) useFilterMapping(mappingImpl *mapping.IndexMappingImpl) {
	mappingImpl.AddCustomCharFilter(`remove_expression_tokens`, map[string]interface{}{
		`type`:   regexp.Name,
		`regexp`: `[\:\[\]\*]+`,
	})

	mappingImpl.AddCustomAnalyzer(`pivot_filter`, map[string]interface{}{
		`type`: custom.Name,
		`char_filters`: []string{
			`remove_expression_tokens`,
		},
		`tokenizer`: single.Name,
		`token_filters`: []string{
			lowercase.Name,
		},
	})

	mappingImpl.DefaultAnalyzer = `pivot_filter`
}
