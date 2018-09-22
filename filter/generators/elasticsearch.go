package generators

import (
	"encoding/json"
	"fmt"

	"github.com/ghetzel/pivot/v3/filter"
)

// Elasticsearch Generator

type Elasticsearch struct {
	filter.Generator
	collection  string
	fields      []string
	criteria    []map[string]interface{}
	options     map[string]interface{}
	values      []interface{}
	facetFields []string
	aggregateBy []filter.Aggregate
}

func NewElasticsearchGenerator() *Elasticsearch {
	return &Elasticsearch{
		Generator: filter.Generator{},
	}
}

func (self *Elasticsearch) Initialize(collectionName string) error {
	self.Reset()
	self.collection = collectionName
	self.fields = make([]string, 0)
	self.criteria = make([]map[string]interface{}, 0)
	self.options = make(map[string]interface{})
	self.values = make([]interface{}, 0)

	return nil
}

func (self *Elasticsearch) Finalize(flt *filter.Filter) error {
	conjunction := `and`

	if flt.Conjunction == filter.OrConjunction {
		conjunction = `or`
	}

	var query map[string]interface{}

	if flt.Spec == `all` {
		query = map[string]interface{}{
			`match_all`: map[string]interface{}{},
		}
	} else {
		query = map[string]interface{}{
			conjunction: self.criteria,
		}
	}

	payload := map[string]interface{}{
		`filter`: query,
		`size`:   flt.Limit,
		`from`:   flt.Offset,
	}

	if len(flt.Fields) > 0 {
		payload[`fields`] = flt.Fields
	}

	if len(flt.Sort) > 0 {
		sorts := make([]interface{}, 0)

		for _, sort := range flt.Sort {
			if len(sort) > 1 && sort[0] == '-' {
				sorts = append(sorts, map[string]interface{}{
					sort[1:]: `desc`,
				})
			} else {
				sorts = append(sorts, map[string]interface{}{
					sort: `asc`,
				})
			}
		}

		payload[`sort`] = sorts
	} else {
		payload[`sort`] = []string{`_doc`}
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

func (self *Elasticsearch) SetOption(key string, value interface{}) error {
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

func (self *Elasticsearch) GetValues() []interface{} {
	return self.values
}

func (self *Elasticsearch) WithCriterion(criterion filter.Criterion) error {
	var c map[string]interface{}
	var err error

	switch criterion.Operator {
	case `is`, ``, `like`:
		c, err = esCriterionOperatorIs(self, criterion)
	case `not`, `unlike`:
		c, err = esCriterionOperatorNot(self, criterion)
	case `contains`, `prefix`, `suffix`:
		c, err = esCriterionOperatorPattern(self, criterion.Operator, criterion)
	case `gt`, `gte`, `lt`, `lte`:
		c, err = esCriterionOperatorRange(self, criterion, criterion.Operator)
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
