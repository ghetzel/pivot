package backends

// this file satifies the Aggregator interface for ElasticsearchIndexer

import (
	"encoding/json"
	"fmt"

	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghetzel/pivot/v3/filter/generators"
)

type esAggregationQuery struct {
	Aggregations map[string]esAggregation `json:"aggs"`
	Query        map[string]interface{}   `json:"query,omitempty"`
	Size         int                      `json:"size"`
	From         int                      `json:"from"`
	Sort         []string                 `json:"sort,omitempty"`
}

type esAggregation map[string]interface{}

func (self *ElasticsearchIndexer) Sum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Sum, field, f)
}

func (self *ElasticsearchIndexer) Count(collection *dal.Collection, flt ...*filter.Filter) (uint64, error) {
	var f *filter.Filter

	if len(flt) > 0 {
		f = flt[0]
	}

	if query, err := filter.Render(
		generators.NewElasticsearchGenerator(),
		collection.GetAggregatorName(),
		f,
	); err == nil {
		var q = maputil.M(query)

		q.Delete(`size`)
		q.Delete(`from`)
		q.Delete(`sort`)

		if res, err := self.client.GetWithBody(
			fmt.Sprintf("/%s/_doc/_count", collection.GetAggregatorName()),
			httputil.Literal(q.JSON()),
			nil,
			nil,
		); err == nil {
			var rv map[string]interface{}

			if err := self.client.Decode(res.Body, &rv); err == nil {
				return uint64(typeutil.Int(rv[`count`])), nil
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

func (self *ElasticsearchIndexer) Minimum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Minimum, field, f)
}

func (self *ElasticsearchIndexer) Maximum(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Maximum, field, f)
}

func (self *ElasticsearchIndexer) Average(collection *dal.Collection, field string, f ...*filter.Filter) (float64, error) {
	return self.aggregateFloat(collection, filter.Average, field, f)
}

func (self *ElasticsearchIndexer) GroupBy(collection *dal.Collection, groupBy []string, aggregates []filter.Aggregate, flt ...*filter.Filter) (*dal.RecordSet, error) {
	if result, err := self.aggregate(collection, groupBy, aggregates, flt, false); err == nil {
		return result.(*dal.RecordSet), nil
	} else {
		return nil, err
	}
}

func (self *ElasticsearchIndexer) aggregateFloat(collection *dal.Collection, aggregation filter.Aggregation, field string, flt []*filter.Filter) (float64, error) {
	if result, err := self.aggregate(collection, nil, []filter.Aggregate{
		{
			Aggregation: aggregation,
			Field:       field,
		},
	}, flt, true); err == nil {
		var aggkey string

		switch aggregation {
		case filter.Minimum:
			aggkey = `min`
		case filter.Maximum:
			aggkey = `max`
		case filter.Sum:
			aggkey = `sum`
		case filter.Average:
			aggkey = `avg`
		}

		if aggkey != `` {
			return maputil.M(result).Float(`aggregations.` + field + `.` + aggkey), nil
		} else {
			return 0, fmt.Errorf("unknown aggregation")
		}
	} else {
		return 0, err
	}
}

func (self *ElasticsearchIndexer) aggregate(collection *dal.Collection, groupBy []string, aggregates []filter.Aggregate, flt []*filter.Filter, single bool) (interface{}, error) {
	var f *filter.Filter

	if len(flt) > 0 {
		f = flt[0]
	}

	if query, err := filter.Render(
		generators.NewElasticsearchGenerator(),
		collection.GetAggregatorName(),
		f,
	); err == nil {
		var esFilter map[string]interface{}

		if err := json.Unmarshal(query, &esFilter); err == nil {
			var aggs = esAggregationQuery{
				Aggregations: make(map[string]esAggregation),
			}

			for _, aggregate := range aggregates {
				var statsKey = aggregate.Field
				var statsField esAggregation

				if s, ok := aggs.Aggregations[statsKey]; ok {
					statsField = s
				} else {
					statsField = make(esAggregation)
				}

				statsField[`stats`] = map[string]interface{}{
					`field`: aggregate.Field,
				}

				aggs.Aggregations[statsKey] = statsField
			}

			if len(esFilter) > 0 {
				aggs.Query = maputil.M(esFilter).Get(`query`).MapNative()
				aggs.Size = 0
			}

			if response, err := self.client.GetWithBody(
				fmt.Sprintf("/%s/_search", collection.GetAggregatorName()),
				&aggs,
				nil,
				nil,
			); err == nil {
				var output = make(map[string]interface{})

				if err := self.client.Decode(response.Body, &output); err == nil {
					return output, nil
				} else {
					return nil, fmt.Errorf("response decode error: %v", err)
				}
			} else {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("filter encode error: %v", err)
		}
	} else {
		return nil, fmt.Errorf("filter error: %v", err)
	}
}

func (self *ElasticsearchIndexer) AggregatorConnectionString() *dal.ConnectionString {
	return self.conn
}

func (self *ElasticsearchIndexer) AggregatorInitialize(parent Backend) error {
	return nil
}
