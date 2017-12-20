package backends

import (
	"encoding/json"

	"github.com/ghetzel/go-stockutil/sliceutil"
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

func (self *MongoBackend) IndexExists(index string, id interface{}) bool {
	return self.Exists(index, id)
}

func (self *MongoBackend) IndexRetrieve(index string, id interface{}) (*dal.Record, error) {
	return self.Retrieve(index, id)
}

func (self *MongoBackend) IndexRemove(index string, ids []interface{}) error {
	return nil
}

func (self *MongoBackend) Index(index string, records *dal.RecordSet) error {
	return nil
}

func (self *MongoBackend) QueryFunc(index string, flt *filter.Filter, resultFn IndexResultFunc) error {
	if collection, err := self.GetCollection(index); err == nil {
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
	} else {
		return err
	}
}

func (self *MongoBackend) Query(index string, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	if f.IdentityField == `` {
		f.IdentityField = MongoIdentityField
	}

	return DefaultQueryImplementation(self, index, f, resultFns...)
}

func (self *MongoBackend) ListValues(index string, fields []string, flt *filter.Filter) (map[string][]interface{}, error) {
	if collection, err := self.GetCollection(index); err == nil {
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
	} else {
		return nil, err
	}
}

func (self *MongoBackend) DeleteQuery(index string, flt *filter.Filter) error {
	if collection, err := self.GetCollection(index); err == nil {
		if query, err := self.filterToNative(collection, flt); err == nil {
			if _, err := self.db.C(collection.Name).RemoveAll(&query); err == nil {
				return nil
			} else {
				return err
			}
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
		collection.Name,
		flt,
	); err == nil {
		var query bson.M

		if err := json.Unmarshal(data, &query); err != nil {
			return nil, err
		}

		return query, nil
	} else {
		return nil, err
	}
}
