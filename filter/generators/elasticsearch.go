package generators

import (
	"encoding/json"
	"fmt"

	"github.com/ghetzel/pivot/v3/filter"
)

// Elasticsearch Generator
var DefaultMinVersionCompat = float64(6)

type Elasticsearch struct {
	filter.Generator
	collection  string
	fields      []string
	criteria    []map[string]any
	options     map[string]any
	values      []any
	facetFields []string
	aggregateBy []filter.Aggregate
	compat      float64
}

func NewElasticsearchGenerator() *Elasticsearch {
	return &Elasticsearch{
		Generator: filter.Generator{},
		compat:    DefaultMinVersionCompat,
	}
}

func (self *Elasticsearch) SetMinimumVersion(v float64) {
	self.compat = v
}

func (self *Elasticsearch) Initialize(collectionName string) error {
	self.Reset()
	self.collection = collectionName
	self.fields = make([]string, 0)
	self.criteria = make([]map[string]any, 0)
	self.options = make(map[string]any)
	self.values = make([]any, 0)

	return nil
}

func (self *Elasticsearch) Finalize(flt *filter.Filter) error {
	var conjunction = `must`

	if flt.Conjunction == filter.OrConjunction {
		conjunction = `should`
	}

	var query map[string]any

	if flt.Spec == `all` {
		query = map[string]any{
			`match_all`: map[string]any{},
		}
	} else {
		query = map[string]any{
			`bool`: map[string]any{
				conjunction: self.criteria,
			},
		}
	}

	var payload = map[string]any{
		`query`: query,
		`size`:  flt.Limit,
		`from`:  flt.Offset,
	}

	if len(flt.Fields) > 0 {
		if self.compat >= 5 {
			payload[`_source`] = map[string]any{
				`include`: flt.Fields,
			}
		} else {
			payload[`fields`] = flt.Fields
		}
	}

	if len(flt.Sort) > 0 {
		var sorts = make([]any, 0)

		for _, sort := range flt.Sort {
			if len(sort) > 1 && sort[0] == '-' {
				sorts = append(sorts, map[string]any{
					sort[1:]: `desc`,
				})
			} else {
				sorts = append(sorts, map[string]any{
					sort: `asc`,
				})
			}
		}

		payload[`sort`] = sorts
	} else {
		payload[`sort`] = []string{`_doc`}
	}

	for k, v := range self.options {
		payload[k] = v
	}

	if data, err := json.MarshalIndent(payload, ``, `    `); err == nil {
		self.Push(data)
	} else {
		return err
	}

	return nil
}

func (self *Elasticsearch) WithField(field string) error {
	self.fields = append(self.fields, field)
	return nil
}

func (self *Elasticsearch) SetOption(key string, value any) error {
	self.options[key] = value
	return nil
}

func (self *Elasticsearch) GroupByField(field string) error {
	self.facetFields = append(self.facetFields, field)
	return nil
}

func (self *Elasticsearch) AggregateByField(agg filter.Aggregation, field string) error {
	self.aggregateBy = append(self.aggregateBy, filter.Aggregate{
		Aggregation: agg,
		Field:       field,
	})

	return nil
}

func (self *Elasticsearch) GetValues() []any {
	return self.values
}

func (self *Elasticsearch) WithCriterion(criterion filter.Criterion) error {
	var c map[string]any
	var err error

	switch criterion.Operator {
	case `is`, ``:
		c, err = esCriterionOperatorIs(self, criterion)
	case `not`:
		c, err = esCriterionOperatorNot(self, criterion)
	case `like`:
		c, err = esCriterionOperatorLike(self, criterion)
	case `unlike`:
		c, err = esCriterionOperatorUnlike(self, criterion)
	case `contains`, `prefix`, `suffix`:
		c, err = esCriterionOperatorPattern(self, criterion.Operator, criterion)
	case `gt`, `gte`, `lt`, `lte`:
		c, err = esCriterionOperatorRange(self, criterion, criterion.Operator)
	case `fulltext`:
		c, err = esCriterionOperatorFulltext(self, criterion)
	default:
		return fmt.Errorf("Unimplemented operator '%s'", criterion.Operator)
	}

	if err != nil {
		return err
	} else {
		self.criteria = append(self.criteria, c)
	}

	return nil
}
