package generators

import (
	"encoding/json"
	"fmt"

	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/v3/filter"
)

// MongoDB Query Generator

type MongoDB struct {
	filter.Generator
	collection  string
	fields      []string
	criteria    []map[string]any
	options     map[string]any
	values      []any
	facetFields []string
	aggregateBy []filter.Aggregate
}

func NewMongoDBGenerator() *MongoDB {
	return &MongoDB{
		Generator: filter.Generator{},
	}
}

func (self *MongoDB) Initialize(collectionName string) error {
	self.Reset()
	self.collection = collectionName
	self.fields = make([]string, 0)
	self.criteria = make([]map[string]any, 0)
	self.options = make(map[string]any)
	self.values = make([]any, 0)

	return nil
}

func (self *MongoDB) Finalize(f *filter.Filter) error {
	conjunction := `$and`

	var query map[string]any

	if f.Conjunction == filter.OrConjunction {
		conjunction = `$or`
	}

	if f.Spec == `all` {
		query = map[string]any{}
	} else if len(self.criteria) == 1 {
		query = self.criteria[0]
	} else {
		query = map[string]any{
			conjunction: self.criteria,
		}
	}

	if data, err := json.MarshalIndent(query, ``, `    `); err == nil {
		self.Push(data)
	} else {
		return err
	}

	return nil
}

func (self *MongoDB) WithField(field string) error {
	if field == `id` {
		field = `_id`
	}

	self.fields = append(self.fields, field)
	return nil
}

func (self *MongoDB) SetOption(key string, value any) error {
	self.options[key] = value
	return nil
}

func (self *MongoDB) GroupByField(field string) error {
	if field == `id` {
		field = `_id`
	}

	self.facetFields = append(self.facetFields, field)
	return nil
}

func (self *MongoDB) AggregateByField(agg filter.Aggregation, field string) error {
	if field == `id` {
		field = `_id`
	}

	self.aggregateBy = append(self.aggregateBy, filter.Aggregate{
		Aggregation: agg,
		Field:       field,
	})

	return nil
}

func (self *MongoDB) GetValues() []any {
	return self.values
}

func (self *MongoDB) WithCriterion(criterion filter.Criterion) error {
	var c map[string]any
	var err error

	if criterion.Field == `id` {
		criterion.Field = `_id`
	}

	for i, value := range criterion.Values {
		switch value.(type) {
		case string:
			criterion.Values[i] = stringutil.Autotype(value)
		}
	}

	switch criterion.Operator {
	case `is`, ``:
		c, err = mongoCriterionOperatorIs(self, criterion)
	case `not`:
		c, err = mongoCriterionOperatorNot(self, criterion)
	case `contains`, `prefix`, `suffix`, `like`, `unlike`:
		c, err = mongoCriterionOperatorPattern(self, criterion.Operator, criterion)
	case `gt`, `gte`, `lt`, `lte`, `range`:
		c, err = mongoCriterionOperatorRange(self, criterion, criterion.Operator)
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
