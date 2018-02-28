package backends

import (
	"fmt"
	"strings"

	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/guregu/dynamo"
)

func (self *DynamoBackend) IndexConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *DynamoBackend) IndexInitialize(Backend) error {
	return nil
}

func (self *DynamoBackend) GetBackend() Backend {
	return self
}

func (self *DynamoBackend) IndexExists(collection *dal.Collection, id interface{}) bool {
	return self.Exists(collection.Name, id)
}

func (self *DynamoBackend) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	return self.Retrieve(collection.Name, id)
}

func (self *DynamoBackend) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return nil
}

func (self *DynamoBackend) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return nil
}

func (self *DynamoBackend) QueryFunc(collection *dal.Collection, flt *filter.Filter, resultFn IndexResultFunc) error {
	if err := self.validateFilter(collection, flt); err != nil {
		return err
	}

	// NOTE: we use the DynamoDB table name instead of .GetIndexName() because we only partially
	// support querying (range scans only), so we're regularly going to be dealing with
	// external indices with potentially-different names.
	table := self.db.Table(collection.Name)

	if flt == nil || flt.IsMatchAll() {
		scan := table.Scan()

		if flt.Limit > 0 {
			scan.Limit(int64(flt.Limit))
		}

		iter := scan.SearchLimit(1).Iter()
		result := make(map[string]interface{})

		for {
			if proceed := iter.Next(&result); proceed {
				if err := iter.Err(); err == nil {
					if proceed, err := self.iterResult(result, collection, flt, resultFn); err == nil {
						if !proceed {
							break
						}
					} else {
						return err
					}
				} else {
					return err
				}

				result = nil

				if lastKey := iter.LastEvaluatedKey(); len(lastKey) > 0 {
					iter = scan.StartFrom(lastKey).SearchLimit(1).Iter()
				} else {
					break
				}
			} else {
				break
			}
		}

	} else {
		var query *dynamo.Query

	IdentityLoop:
		for _, criterion := range flt.Criteria {
			if query == nil && collection.IsIdentityField(criterion.Field) {
				switch len(criterion.Values) {
				case 1:
					query = table.Get(criterion.Field, criterion.Values[0])
					break IdentityLoop
				default:
					return fmt.Errorf("Only one primary key value is supported when querying DynamoDB at this time")
				}
			}
		}

		if query == nil {
			return fmt.Errorf("Could not generate DynamoDB query from filter %v", flt)
		}

		for _, criterion := range flt.Criteria {
			if !collection.IsIdentityField(criterion.Field) {
				values := make([]interface{}, 0)
				orFilters := make([]string, 0)

				for _, v := range criterion.Values {
					nativeOp := self.toNativeOp(&criterion)

					if nativeOp == `` {
						return fmt.Errorf("Unsupported operator '%v' when querying DynamoDB", criterion.Operator)
					}

					orFilters = append(
						orFilters,
						fmt.Sprintf("$ %s ?", nativeOp),
					)

					values = append(values, criterion.Field)
					values = append(values, v)
				}

				var filterExpr string

				if len(orFilters) == 1 {
					filterExpr = orFilters[0]
				} else if len(orFilters) > 1 {
					filterExpr = `(` + strings.Join(orFilters, ` OR `) + `)`
				} else {
					continue
				}

				querylog.Debugf("[%T] Table: %v; FILTER: %v %+v", self, collection.Name, filterExpr, values)
				query = query.Filter(filterExpr, values...)
			}
		}

		if flt.Limit > 0 {
			query.Limit(int64(flt.Limit))
		}

		iter := query.SearchLimit(1).Iter()
		result := make(map[string]interface{})

		for {
			if proceed := iter.Next(&result); proceed {
				if err := iter.Err(); err == nil {
					if proceed, err := self.iterResult(result, collection, flt, resultFn); err == nil {
						if !proceed {
							break
						}
					} else {
						return err
					}
				} else {
					return err
				}

				result = nil

				if lastKey := iter.LastEvaluatedKey(); len(lastKey) > 0 {
					iter = query.StartFrom(lastKey).SearchLimit(1).Iter()
				} else {
					break
				}
			} else {
				break
			}
		}
	}

	return nil
}

func (self *DynamoBackend) Query(collection *dal.Collection, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	if f != nil {
		f.Options[`ForceIndexRecord`] = true
	}

	return DefaultQueryImplementation(self, collection, f, resultFns...)
}

func (self *DynamoBackend) ListValues(collection *dal.Collection, fields []string, flt *filter.Filter) (map[string][]interface{}, error) {
	return nil, fmt.Errorf("%T.ListValues: Not Implemented", self)
}

func (self *DynamoBackend) DeleteQuery(collection *dal.Collection, flt *filter.Filter) error {
	return fmt.Errorf("%T.DeleteQuery: Not Implemented", self)
}

func (self *DynamoBackend) FlushIndex() error {
	return nil
}

func (self *DynamoBackend) validateFilter(collection *dal.Collection, flt *filter.Filter) error {
	if flt != nil {
		for _, field := range flt.Fields {
			if collection.IsIdentityField(field) {
				continue
			}

			if collection.IsKeyField(field) {
				continue
			}

			return fmt.Errorf("Filter field '%v' cannot be used: not a key field", field)
		}
	}

	return nil
}

func (self *DynamoBackend) toNativeOp(criterion *filter.Criterion) string {
	switch criterion.Operator {
	case `not`:
		return `<>`
	case `lt`:
		return `<`
	case `lte`:
		return `<=`
	case `gt`:
		return `>`
	case `gte`:
		return `>=`
	case `is`, ``:
		return `=`
	default:
		return ``
	}
}

func (self *DynamoBackend) iterResult(result map[string]interface{}, collection *dal.Collection, flt *filter.Filter, resultFn IndexResultFunc) (bool, error) {
	record := dal.NewRecord(nil)

	for k, v := range result {
		if collection.IsIdentityField(k) {
			record.ID = v
		} else {
			record.Fields[k] = v
		}
	}

	if err := resultFn(record, nil, IndexPage{
		Limit: flt.Limit,
	}); err != nil {
		return false, err
	}

	return true, nil
}
