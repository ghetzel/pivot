package backends

// this file satifies the Aggregator interface for MongoBackend

import (
	"fmt"
	"strings"
	"time"

	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"gopkg.in/mgo.v2/bson"
)

func (self *MongoBackend) Sum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Sum, field, f)
}

func (self *MongoBackend) Count(collection *dal.Collection, flt ...*filter.Filter) (uint64, error) {
	var f *filter.Filter

	if len(flt) > 0 {
		f = flt[0]
	}

	if query, err := self.filterToNative(collection, f); err == nil {
		q := self.db.C(collection.Name).Find(query)

		if totalResults, err := q.Count(); err == nil {
			return uint64(totalResults), nil
		} else {
			return 0, err
		}
	} else {
		return 0, err
	}
}

func (self *MongoBackend) Minimum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Minimum, field, f)
}

func (self *MongoBackend) Maximum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Maximum, field, f)
}

func (self *MongoBackend) Average(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Average, field, f)
}

func (self *MongoBackend) GroupBy(collection *dal.Collection, groupBy []string, aggregates []filter.Aggregate, flt ...*filter.Filter) (*dal.RecordSet, error) {
	if result, err := self.aggregate(collection, groupBy, aggregates, flt, false); err == nil {
		return result.(*dal.RecordSet), nil
	} else {
		return nil, err
	}
}

func (self *MongoBackend) aggregateFloat(collection *dal.Collection, aggregation filter.Aggregation, field string, flt []*filter.Filter) (float64, error) {
	if result, err := self.aggregate(collection, nil, []filter.Aggregate{
		{
			Aggregation: aggregation,
			Field:       field,
		},
	}, flt, true); err == nil {
		if vF, ok := result.(float64); ok {
			return vF, nil
		} else {
			return 0, err
		}
	} else {
		return 0, err
	}
}

func (self *MongoBackend) aggregate(collection *dal.Collection, groupBy []string, aggregates []filter.Aggregate, flt []*filter.Filter, single bool) (interface{}, error) {
	var f *filter.Filter

	if len(flt) > 0 {
		f = flt[0]
	}

	if query, err := self.filterToNative(collection, f); err == nil {
		var aggGroups []bson.M
		var firstKey string

		for _, aggregate := range aggregates {
			var mongoFn string

			switch aggregate.Aggregation {
			case filter.Sum:
				mongoFn = `$sum`
			case filter.First:
				mongoFn = `$first`
			case filter.Last:
				mongoFn = `$last`
			case filter.Minimum:
				mongoFn = `$min`
			case filter.Maximum:
				mongoFn = `$max`
			case filter.Average:
				mongoFn = `$avg`
			}

			aggGroups = append(aggGroups, bson.M{
				`$group`: bson.M{
					`_id`: aggregate.Field,
					strings.TrimPrefix(mongoFn, `$`): bson.M{
						mongoFn: fmt.Sprintf("$%s", aggregate.Field),
					},
				},
			})

			if firstKey == `` {
				firstKey = strings.TrimPrefix(mongoFn, `$`)
			}
		}

		var finalQuery []bson.M

		if len(query) > 0 {
			finalQuery = append(finalQuery, query)
		}

		finalQuery = append(finalQuery, aggGroups...)

		q := self.db.C(collection.Name).Pipe(finalQuery)
		iter := q.Iter()

		var result map[string]interface{}

		for iter.Next(&result) {
			if err := iter.Err(); err != nil {
				return nil, err
			} else if single {
				_id, _ := result[`_id`]

				if v, ok := result[firstKey]; ok {
					if vF, err := stringutil.ConvertToFloat(v); err == nil {
						return vF, nil
					} else if vT, err := stringutil.ConvertToTime(v); err == nil {
						return float64(vT.UnixNano()) / float64(time.Second), nil
					} else {
						return 0, fmt.Errorf("'%s' aggregation not supported for field %v", firstKey, _id)
					}
				} else {
					return 0, fmt.Errorf("missing aggregation value '%s'", firstKey)
				}
			} else {
				return nil, fmt.Errorf("Not implemented")
			}
		}

		return nil, nil
	} else {
		return nil, fmt.Errorf("filter error: %v", err)
	}
}

func (self *MongoBackend) AggregatorConnectionString() *dal.ConnectionString {
	return self.GetConnectionString()
}

func (self *MongoBackend) AggregatorInitialize(parent Backend) error {
	return nil
}
