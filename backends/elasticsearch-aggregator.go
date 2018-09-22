package backends

// this file satifies the Aggregator interface for ElasticsearchIndexer

import (
	"encoding/json"
	"fmt"

	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghetzel/pivot/v3/filter/generators"
)

type esAggregationQuery struct {
	Aggregations map[string]esAggregation `json:"aggs"`
	Filter       map[string]interface{}   `json:"filter,omitempty"`
	Size         int                      `json:"size"`
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

	f.Limit = 1

	var count uint64
	var wasSet bool

	if _, err := self.Query(collection, f, func(_ *dal.Record, err error, page IndexPage) error {
		if err == nil {
			if !wasSet {
				count = uint64(page.TotalResults)
				wasSet = true
			}

			return nil
		} else {
			return err
		}
	}); err == nil {
		return count, nil
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
		return result.(float64), nil
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
			aggs := esAggregationQuery{
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
				aggs.Filter = esFilter
				aggs.Size = 0
			}

			if query, err := json.Marshal(aggs); err == nil {
				if req, err := self.newRequest(`GET`, fmt.Sprintf("/%s/_search", collection.GetAggregatorName()), string(query)); err == nil {
					// perform request, read response
					if response, err := self.client.Do(req); err == nil {
						if response.StatusCode < 400 {
							output := make(map[string]interface{})

							if err := json.NewDecoder(response.Body).Decode(&output); err == nil {
								return output, nil
							} else {
								return nil, fmt.Errorf("response decode error: %v", err)
							}
						} else {
							return nil, fmt.Errorf("Got HTTP %v", response.Status)
						}
					} else {
						return nil, err
					}

				} else {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("filter encode error: %v", err)
			}
		} else {
			return nil, fmt.Errorf("filter decode error: %v", err)
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
