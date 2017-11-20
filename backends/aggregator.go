package backends

import (
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type Aggregator interface {
	AggregatorConnectionString() *dal.ConnectionString
	AggregatorInitialize(Backend) error
	Sum(collection string, field string, f ...*filter.Filter) (float64, error)
	Count(collection string, f ...*filter.Filter) (uint64, error)
	Minimum(collection string, field string, f ...*filter.Filter) (float64, error)
	Maximum(collection string, field string, f ...*filter.Filter) (float64, error)
	Average(collection string, field string, f ...*filter.Filter) (float64, error)
	GroupBy(collection string, fields []string, aggregates []filter.Aggregate, f ...*filter.Filter) (*dal.RecordSet, error)
}
