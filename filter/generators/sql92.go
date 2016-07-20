package generators

import (
	"fmt"
	"github.com/ghetzel/pivot/filter"
	"strings"
)

// SQL-92 Generator

type Sql92 struct {
	filter.Generator
	collection string
	fields     []string
	criteria   []string
}

func NewSql92Generator() *Sql92 {
	return &Sql92{
		Generator: filter.Generator{},
	}
}

func (self *Sql92) Initialize(collectionName string) error {
	self.collection = collectionName
	self.fields = make([]string, 0)
	self.criteria = make([]string, 0)

	return nil
}

func (self *Sql92) Finalize(filter filter.Filter) error {
	self.Push([]byte(`SELECT `))

	if len(self.fields) == 0 {
		self.Push([]byte(`*`))
	} else {
		self.Push([]byte(strings.Join(self.fields, `,`)))
	}

	self.Push([]byte(` FROM `))
	self.Push([]byte(self.collection))

	if len(self.criteria) > 0 {
		self.Push([]byte(` `))

		for _, criterionStr := range self.criteria {
			self.Push([]byte(criterionStr))
			self.Push([]byte(` `))
		}
	}

	return nil
}

func (self *Sql92) WithField(field string) error {
	self.fields = append(self.fields, field)
	return nil
}

func (self *Sql92) SetOption(key, value string) error {
	return nil
}

func (self *Sql92) WithCriterion(criterion filter.Criterion) error {
	criterionStr := ``

	if len(self.criteria) == 0 {
		criterionStr = `WHERE (`
	} else {
		criterionStr = `AND (`
	}

	outValues := make([]string, 0)

	for _, value := range criterion.Values {
		outVal := ``

		if criterion.Type != `` {
			if criterionType, err := self.filterTypeToSqlType(criterion.Type, criterion.Length); err == nil {
				outVal = outVal + fmt.Sprintf("CAST(%s AS %s)", criterion.Field, criterionType)
			} else {
				return err
			}
		} else {
			outVal = outVal + criterion.Field
		}

		switch criterion.Operator {
		case `is`, ``:
			if value == `null` {
				outVal = outVal + ` IS NULL`
			} else {
				outVal = outVal + fmt.Sprintf(" = %s", value)
			}
		case `not`:
			if value == `null` {
				outVal = outVal + ` IS NOT NULL`
			} else {
				outVal = outVal + fmt.Sprintf(" <> %s", value)
			}
		case `contains`:
			outVal = outVal + fmt.Sprintf(" LIKE '%%%s%%'", value)
		case `gt`:
			outVal = outVal + fmt.Sprintf(" > %s", value)
		case `gte`:
			outVal = outVal + fmt.Sprintf(" >= %s", value)
		case `lt`:
			outVal = outVal + fmt.Sprintf(" < %s", value)
		case `lte`:
			outVal = outVal + fmt.Sprintf(" <= %s", value)
		default:
			return fmt.Errorf("Unimplemented operator '%s'", criterion.Operator)
		}

		outValues = append(outValues, outVal)
	}

	criterionStr = criterionStr + strings.Join(outValues, ` OR `) + `)`

	self.criteria = append(self.criteria, criterionStr)

	return nil
}

func (self *Sql92) filterTypeToSqlType(in string, length int) (string, error) {
	out := ``

	switch strings.ToLower(in) {
	case `str`:
		out = `VARCHAR`
	case `int`:
		out = `INT`
	case `float`:
		out = `FLOAT`
	case `bool`:
		out = `TINYINT`
		length = 1
	case `date`:
		out = `DATETIME`
	default:
		out = in
	}

	if length > 0 {
		out = out + fmt.Sprintf("(%d)", length)
	}

	return strings.ToUpper(out), nil
}
