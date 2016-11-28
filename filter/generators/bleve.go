package generators

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/filter"
	"strings"
)

// Golang Bleve Generator (see: http://www.blevesearch.com/docs/Query-String-Query/)

type Bleve struct {
	filter.Generator
	collection string
	docID []string
	matchAll bool
	matchPhrase []string
	matchPrefix []string
	matchRanges [][]float64
	matchRegexp []string
	matchTerms []string
	matchWildcard []string
}

func NewBleveGenerator() *Bleve {
	return &Bleve{
		Generator: filter.Generator{},
	}
}

func (self *Bleve) Initialize(collectionName string) error {
	self.collection = collectionName
	self.docID = make([]string, 0)
	self.matchPhrase = make([]string, 0)
	self.matchPrefix = make([]string, 0)
	self.matchRanges = make([][]float64, 0)
	self.matchRegexp = make([]string, 0)
	self.matchTerms = make([]string, 0)
	self.matchWildcard = make([]string, 0)

	return nil
}

func (self *Bleve) Finalize(filter filter.Filter) error {
	serialized := make(map[string]interface{})

	if self.matchAll {
		serialized[`match_all`] = true
	}else{
		serialized[`id`] = self.docID
		serialized[`phrase`] = self.matchPhrase
		serialized[`prefix`] = self.matchPrefix
		serialized[`range`] = self.matchRanges
		serialized[`regexp`] = self.matchRegexp
		serialized[`term`] = self.matchTerm
		serialized[`wildcard`] = self.matchWildcard
	}

	return nil
}

func (self *Bleve) WithField(field string) error {
	return nil
}

func (self *Bleve) SetOption(key, value string) error {
	return nil
}

func (self *Bleve) WithCriterion(criterion filter.Criterion) error {
	for _, value := range criterion.Values {

		switch criterion.Operator {
		case `is`, ``:
			if value == `null` {
				self.must = append(self.must, fmt.Sprintf("%s:\"\"", criterion.Field))
			} else {
				self.must = append(self.must, fmt.Sprintf("%s:%v", criterion.Field, value))
			}
		case `contains`, `prefix`, `suffix`:
			if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
				self.must = append(self.must, fmt.Sprintf("%s:%v", criterion.Field, value))
			} else {
				self.must = append(self.must, fmt.Sprintf("%s:%q", criterion.Field, value))
			}
		case `not`:
			if value == `null` {
				self.mustNot = append(self.mustNot, fmt.Sprintf("%s:\"\"", criterion.Field))
			} else {
				self.mustNot = append(self.mustNot, fmt.Sprintf("%s:%v", criterion.Field, value))
			}
		case `gt`:
			op = `>`
		case `gte`:
			op = `>=`
		case `lt`:
			op = `<`
		case `lte`:
			op = `<=`
		default:
			return fmt.Errorf("Unimplemented operator '%s'", criterion.Operator)
		}

		if op != `` {
			if v, err := stringutil.ConvertToInteger(value); err == nil {
				self.must = append(self.must, fmt.Sprintf("%s:%s%d", criterion.Field, op, v))
			} else if v, err := stringutil.ConvertToFloat(value); err == nil {
				self.must = append(self.must, fmt.Sprintf("%s:%s%f", criterion.Field, op, v))
			} else {
				return err
			}
		}
	}

	return nil
}
