package generators

import (
	"encoding/json"
	"fmt"

	"github.com/ghetzel/pivot/filter"
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

func (self *Elasticsearch) Finalize(filter filter.Filter) error {
	var query map[string]interface{}

	if filter.Spec == `all` {
		query = map[string]interface{}{
			`match_all`: map[string]interface{}{},
		}
	} else {
		query = map[string]interface{}{
			`and`: self.criteria,
		}
	}

	if data, err := json.MarshalIndent(query, ``, `    `); err == nil {
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
	case `is`, ``:
		c, err = esCriterionOperatorIs(self, criterion)
	case `not`:
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
