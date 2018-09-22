package backends

import (
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type Aggregator interface {
	AggregatorConnectionString() *dal.ConnectionString
	AggregatorInitialize(Backend) error
	Sum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error)
	Count(collection *dal.Collection, f ...*filter.Filter) (uint64, error)
	Minimum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error)
	Maximum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error)
	Average(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error)
	GroupBy(collection *dal.Collection, fields []string, aggregates []filter.Aggregate, f ...*filter.Filter) (*dal.RecordSet, error)
}
