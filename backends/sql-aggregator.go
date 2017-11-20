package backends

// this file satifies the Aggregator interface for SqlBackend

import (
	"database/sql"
	"reflect"

	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
)

type sqlAggResultFunc func(*sql.Rows, *generators.Sql, *dal.Collection, *filter.Filter) (interface{}, error)

func (self *SqlBackend) Sum(collection string, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Sum, field, f)
}

func (self *SqlBackend) Count(collection string, f ...*filter.Filter) (uint64, error) {
	v, err := self.aggregateFloat(collection, filter.Count, `*`, f)
	return uint64(v), err
}

func (self *SqlBackend) Minimum(collection string, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Minimum, field, f)
}

func (self *SqlBackend) Maximum(collection string, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Maximum, field, f)
}

func (self *SqlBackend) Average(collection string, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Average, field, f)
}

func (self *SqlBackend) GroupBy(collection string, groupBy []string, aggregates []filter.Aggregate, f ...*filter.Filter) (*dal.RecordSet, error) {
	if result, err := self.aggregate(collection, groupBy, aggregates, f, self.extractRecordSet); err == nil {
		return result.(*dal.RecordSet), nil
	} else {
		return nil, err
	}
}

func (self *SqlBackend) aggregateFloat(name string, aggregation filter.Aggregation, field string, f []*filter.Filter) (float64, error) {
	if result, err := self.aggregate(name, nil, []filter.Aggregate{
		{
			Aggregation: aggregation,
			Field:       field,
		},
	}, f, self.extractSingleFloat64); err == nil {
		return result.(float64), nil
	} else {
		return 0, err
	}
}

func (self *SqlBackend) aggregate(name string, groupBy []string, aggregates []filter.Aggregate, f []*filter.Filter, resultFn sqlAggResultFunc) (interface{}, error) {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		queryGen := self.makeQueryGen(collection)
		var flt *filter.Filter

		if len(f) == 0 {
			flt = filter.New()
		} else {
			flt = f[0]
		}

		for _, g := range groupBy {
			queryGen.GroupByField(g)
		}

		for _, agg := range aggregates {
			queryGen.AggregateByField(agg.Aggregation, agg.Field)
		}

		if err := queryGen.Initialize(collection.Name); err == nil {
			if stmt, err := filter.Render(queryGen, collection.Name, flt); err == nil {
				querylog.Debugf("[%T] %s %v", self, string(stmt[:]), queryGen.GetValues())

				// perform query
				if rows, err := self.db.Query(string(stmt[:]), queryGen.GetValues()...); err == nil {
					defer rows.Close()
					return resultFn(rows, queryGen, collection, flt)
				} else {
					return nil, err
				}
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

func (self *SqlBackend) AggregatorConnectionString() *dal.ConnectionString {
	return self.GetConnectionString()
}

func (self *SqlBackend) AggregatorInitialize(parent Backend) error {
	return nil
}

func (self *SqlBackend) extractSingleFloat64(rows *sql.Rows, _ *generators.Sql, _ *dal.Collection, _ *filter.Filter) (interface{}, error) {
	if rows.Next() {
		var rv sql.NullFloat64

		if err := rows.Scan(&rv); err == nil {
			return rv.Float64, nil
		} else {
			return float64(0), err
		}
	} else {
		return float64(0), nil
	}
}

func (self *SqlBackend) extractRecordSet(rows *sql.Rows, queryGen *generators.Sql, collection *dal.Collection, flt *filter.Filter) (interface{}, error) {
	recordset := dal.NewRecordSet()

	if columns, err := rows.Columns(); err == nil {
		for rows.Next() {
			if record, err := self.scanFnValueToRecord(queryGen, collection, columns, reflect.ValueOf(rows.Scan), flt.Fields); err == nil {
				recordset.Push(record)
			} else {
				return nil, err
			}
		}
	} else {
		return nil, err
	}

	return recordset, nil
}
