package backends

import (
	"encoding/json"
	"fmt"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
	"gopkg.in/mgo.v2/bson"
)

func (self *MongoBackend) IndexConnectionString() *dal.ConnectionString {
	return self.conn
}

func (self *MongoBackend) IndexInitialize(Backend) error {
	return nil
}

func (self *MongoBackend) GetBackend() Backend {
	return self
}

func (self *MongoBackend) IndexExists(collection *dal.Collection, id interface{}) bool {
	return self.Exists(collection.GetIndexName(), id)
}

func (self *MongoBackend) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	return self.Retrieve(collection.GetIndexName(), id)
}

func (self *MongoBackend) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return nil
}

func (self *MongoBackend) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return nil
}

func (self *MongoBackend) QueryFunc(collection *dal.Collection, flt *filter.Filter, resultFn IndexResultFunc) error {
	var result map[string]interface{}

	if query, err := self.filterToNative(collection, flt); err == nil {
		q := self.db.C(collection.Name).Find(query)

		if totalResults, err := q.Count(); err == nil {
			if flt.Limit > 0 {
				q.Limit(flt.Limit)
			}

			if flt.Offset > 0 {
				q.Skip(flt.Offset)
			}

			if len(flt.Sort) > 0 {
				q.Sort(flt.Sort...)
			}

			iter := q.Iter()

			for iter.Next(&result) {
				if err := iter.Err(); err != nil {
					return err
				} else {
					if record, err := self.recordFromResult(collection, result, flt.Fields...); err == nil {
						if err := resultFn(record, nil, IndexPage{
							Limit:        flt.Limit,
							Offset:       flt.Offset,
							TotalResults: int64(totalResults),
						}); err != nil {
							return err
						}

						result = nil
					} else {
						return err
					}
				}
			}

			return iter.Close()
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *MongoBackend) Query(collection *dal.Collection, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	if f != nil {
		if f.IdentityField == `` {
			f.IdentityField = MongoIdentityField
		}

		f.Options[`ForceIndexRecord`] = true
	}

	return DefaultQueryImplementation(self, collection, f, resultFns...)
}

func (self *MongoBackend) ListValues(collection *dal.Collection, fields []string, flt *filter.Filter) (map[string][]interface{}, error) {
	if query, err := self.filterToNative(collection, flt); err == nil {
		rv := make(map[string][]interface{})

		for _, field := range fields {
			qfield := field

			if qfield == `id` {
				qfield = MongoIdentityField
			}

			var results []interface{}

			if err := self.db.C(collection.Name).Find(&query).Distinct(qfield, &results); err == nil {
				rv[field] = sliceutil.Autotype(results)
			} else {
				return nil, err
			}
		}

		return rv, nil
	} else {
		return nil, err
	}
}

func (self *MongoBackend) DeleteQuery(collection *dal.Collection, flt *filter.Filter) error {
	if query, err := self.filterToNative(collection, flt); err == nil {
		if _, err := self.db.C(collection.Name).RemoveAll(&query); err == nil {
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *MongoBackend) FlushIndex() error {
	return nil
}

func (self *MongoBackend) filterToNative(collection *dal.Collection, flt *filter.Filter) (bson.M, error) {
	if data, err := filter.Render(
		generators.NewMongoDBGenerator(),
		collection.GetIndexName(),
		flt,
	); err == nil {
		var query bson.M

		if err := json.Unmarshal(data, &query); err != nil {
			return nil, err
		}

		// handle type-specific processing of values; nuances that get lost in the JSON-to-Map serialization
		// process.  I *wanted* to just serialize to BSON directly from the query generator interface, but
		// that turned into a messy time-wasting boondoggle #neat.
		//
		query = bson.M(maputil.Apply(query, func(key []string, value interface{}) (interface{}, bool) {
			vS := fmt.Sprintf("%v", value)

			if bson.IsObjectIdHex(vS) {
				return bson.ObjectIdHex(vS), true
			} else if vT, err := stringutil.ConvertToTime(value); err == nil {
				return vT, true
			} else {
				return nil, false
			}
		}))

		querylog.Debugf("[%T] query: %v", self, typeutil.Dump(query))
		return query, nil
	} else {
		return nil, err
	}
}
