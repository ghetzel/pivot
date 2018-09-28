package backends

import (
	"fmt"
	"math"

	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/gomodule/redigo/redis"
)

func (self *RedisBackend) IndexConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *RedisBackend) IndexInitialize(Backend) error {
	return nil
}

func (self *RedisBackend) GetBackend() Backend {
	return self
}

func (self *RedisBackend) IndexExists(collection *dal.Collection, id interface{}) bool {
	return self.Exists(collection.Name, id)
}

func (self *RedisBackend) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	return self.Retrieve(collection.Name, id)
}

func (self *RedisBackend) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return nil
}

func (self *RedisBackend) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return nil
}

func (self *RedisBackend) QueryFunc(collection *dal.Collection, flt *filter.Filter, resultFn IndexResultFunc) error {
	if err := self.validateFilter(collection, flt); err != nil {
		return err
	}

	var keyPattern string
	placeholders := make([]interface{}, 0)

	for i := 0; i < len(collection.Keys()); i++ {
		placeholders = append(placeholders, `*`)
	}

	// full keyscan
	if flt == nil || flt.IsMatchAll() {
		keyPattern = self.key(collection, placeholders)
	} else {
		for i, criterion := range flt.Criteria {
			if !criterion.IsExactMatch() || len(criterion.Values) != 1 {
				return fmt.Errorf(
					"%v: filters can only contain exact match criteria (%q is invalid on %d values)",
					self,
					criterion.Operator,
					len(criterion.Values),
				)
			} else if i < len(placeholders) {
				placeholders[i] = fmt.Sprintf("%v", criterion.Values[0])
			} else {
				return fmt.Errorf("%v: too many criteria", self)
			}
		}
	}

	if keys, err := redis.Strings(self.run(`KEYS`, keyPattern)); err == nil {
		limit := len(keys)
		total := int64(len(keys))

		if flt.Limit > 0 && flt.Limit < limit {
			limit = flt.Limit
		}

		for i := flt.Offset; i < limit; i++ {
			if _, values := redisSplitKey(keys[i]); len(values) > 0 {
				record, err := self.Retrieve(collection.Name, values, flt.Fields...)

				// fire off the result handler
				if err := resultFn(record, err, IndexPage{
					Page:         1,
					TotalPages:   int(math.Ceil(float64(total) / float64(flt.Limit))),
					Limit:        flt.Limit,
					Offset:       (1 - 1) * flt.Limit,
					TotalResults: total,
				}); err != nil {
					return err
				}
			}
		}

		return nil
	} else {
		return err
	}
}

func (self *RedisBackend) Query(collection *dal.Collection, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	if f != nil {
		f.Options[`ForceIndexRecord`] = true
	}

	return DefaultQueryImplementation(self, collection, f, resultFns...)
}

func (self *RedisBackend) ListValues(collection *dal.Collection, fields []string, flt *filter.Filter) (map[string][]interface{}, error) {
	return nil, fmt.Errorf("%T.ListValues: Not Implemented", self)
}

func (self *RedisBackend) DeleteQuery(collection *dal.Collection, flt *filter.Filter) error {
	return fmt.Errorf("%T.DeleteQuery: Not Implemented", self)
}

func (self *RedisBackend) FlushIndex() error {
	return nil
}

func (self *RedisBackend) validateFilter(collection *dal.Collection, flt *filter.Filter) error {
	if flt != nil {
		for _, field := range flt.CriteriaFields() {
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

// func (self *RedisBackend) iterResult(collection *dal.Collection, flt *filter.Filter, items []map[string]*dynamodb.AttributeValue, processed int, totalResults int64, pageNumber int, lastPage bool, resultFn IndexResultFunc) bool {
// 	if len(items) > 0 {
// 		for _, item := range items {
// 			record, err := dynamoRecordFromItem(collection, item)

// 			// fire off the result handler
// 			if err := resultFn(record, err, IndexPage{
// 				Page:         pageNumber,
// 				TotalPages:   int(math.Ceil(float64(totalResults) / float64(25))),
// 				Limit:        flt.Limit,
// 				Offset:       (pageNumber - 1) * 25,
// 				TotalResults: totalResults,
// 			}); err != nil {
// 				return false
// 			}

// 			// perform bounds checking
// 			if processed += 1; flt.Limit > 0 && processed >= flt.Limit {
// 				return false
// 			}
// 		}

// 		return !lastPage
// 	} else {
// 		return false
// 	}
// }
