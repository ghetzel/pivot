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
	must       []string
	mustNot    []string
}

func NewBleveGenerator() *Bleve {
	return &Bleve{
		Generator: filter.Generator{},
	}
}

func (self *Bleve) Initialize(collectionName string) error {
	self.collection = collectionName
	self.must = make([]string, 0)
	self.mustNot = make([]string, 0)

	return nil
}

func (self *Bleve) Finalize(filter filter.Filter) error {
	for i, c := range self.must {
		self.must[i] = `+` + c
	}

	for i, c := range self.mustNot {
		self.mustNot[i] = `-` + c
	}

	if len(self.must) > 0 {
		line := strings.Join(self.must, ` `)
		self.Push([]byte(line[:]))

		if len(self.mustNot) > 0 {
			self.Push([]byte{' '})
		}
	}

	if len(self.mustNot) > 0 {
		line := strings.Join(self.mustNot, ` `)
		self.Push([]byte(line[:]))
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
		op := ``

		switch criterion.Operator {
		case `prefix`:
			value = value + `*`
		case `suffix`:
			value = `*` + value
		case `contains`:
			value = `*` + value + `*`
		}

		// quote strings
		if criterion.Type == `str` || strings.Contains(value, ` `) {
			value = fmt.Sprintf("%q", value)
		}

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
