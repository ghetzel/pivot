package backends

// this file satifies the Aggregator interface for SqlBackend

import (
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

func (self *SqlBackend) Sum(name string, field string, f filter.Filter) (float64, error) {
	return self.aggregate(name, field, f, "SUM(%s)")
}

func (self *SqlBackend) Count(name string, f filter.Filter) (uint64, error) {
	v, err := self.aggregate(name, ``, f, "COUNT(1)%s")
	return uint64(v), err
}

func (self *SqlBackend) Minimum(name string, field string, f filter.Filter) (float64, error) {
	return self.aggregate(name, field, f, "MIN(%s)")
}

func (self *SqlBackend) Maximum(name string, field string, f filter.Filter) (float64, error) {
	return self.aggregate(name, field, f, "MAX(%s)")
}

func (self *SqlBackend) Average(name string, field string, f filter.Filter) (float64, error) {
	return self.aggregate(name, field, f, "AVG(%s)")
}

func (self *SqlBackend) AggregatorConnectionString() *dal.ConnectionString {
	return self.GetConnectionString()
}

func (self *SqlBackend) AggregatorInitialize(parent Backend) error {
	return nil
}

func (self *SqlBackend) aggregate(name string, field string, f filter.Filter, format string) (float64, error) {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		queryGen := self.makeQueryGen(collection)

		f.Fields = []string{field}
		queryGen.FieldWrappers[field] = format

		if err := queryGen.Initialize(collection.Name); err == nil {
			if stmt, err := filter.Render(queryGen, collection.Name, f); err == nil {
				querylog.Debugf("[%T] %s %v", self, string(stmt[:]), queryGen.GetValues())

				// perform query
				if rows, err := self.db.Query(string(stmt[:]), queryGen.GetValues()...); err == nil {
					defer rows.Close()

					if rows.Next() {
						var rv float64

						if err := rows.Scan(&rv); err == nil {
							return rv, nil
						} else {
							return 0, err
						}
					} else {
						return 0, nil
					}
				} else {
					return 0, err
				}
			} else {
				return 0, err
			}
		} else {
			return 0, err
		}
	} else {
		return 0, err
	}
}
