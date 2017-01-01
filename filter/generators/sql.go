package generators

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/filter"
	"strings"
)

// SQL Generator

type SqlStatementType int

const (
	SqlSelectStatement SqlStatementType = iota
	SqlInsertStatement
	SqlUpdateStatement
)

type SqlTypeMapping struct {
	StringType        string
	StringTypeLength  int
	IntegerType       string
	FloatType         string
	BooleanType       string
	BooleanTypeLength int
	DateTimeType      string
}

var CassandraTypeMapping = SqlTypeMapping{
	StringType:        `VARCHAR`,
	IntegerType:       `INT`,
	FloatType:         `FLOAT`,
	BooleanType:       `TINYINT`,
	BooleanTypeLength: 1,
	DateTimeType:      `DATETIME`,
}

var MysqlTypeMapping = SqlTypeMapping{
	StringType:       `VARCHAR`,
	StringTypeLength: 255,
	IntegerType:      `BIGINT`,
	FloatType:        `DECIMAL`,
	BooleanType:      `BOOL`,
	DateTimeType:     `DATETIME`,
}

var SqliteTypeMapping = SqlTypeMapping{
	StringType:   `TEXT`,
	IntegerType:  `INTEGER`,
	FloatType:    `REAL`,
	BooleanType:  `INTEGER`,
	DateTimeType: `INTEGER`,
}

var DefaultSqlTypeMapping = MysqlTypeMapping

type Sql struct {
	filter.Generator
	UsePlaceholders     bool
	PlaceholderFormat   string
	PlaceholderArgument string // if specified, either "index", "index1" or "field"
	UnquotedValueFormat string
	QuotedValueFormat   string
	FieldNameFormat     string
	TypeMapping         SqlTypeMapping
	Type                SqlStatementType
	collection          string
	fields              []string
	criteria            []string
	criteriaFields      []string
	criteriaValues      []string
	values              []interface{}
}

func NewSqlGenerator() *Sql {
	return &Sql{
		Generator:           filter.Generator{},
		PlaceholderFormat:   `?`,
		PlaceholderArgument: ``,
		UnquotedValueFormat: "%s",
		QuotedValueFormat:   "'%s'",
		FieldNameFormat:     "%s",
		TypeMapping:         DefaultSqlTypeMapping,
		Type:                SqlSelectStatement,
	}
}

func (self *Sql) Initialize(collectionName string) error {
	self.Reset()
	self.collection = collectionName
	self.fields = make([]string, 0)
	self.criteria = make([]string, 0)
	self.criteriaFields = make([]string, 0)
	self.criteriaValues = make([]string, 0)
	self.values = make([]interface{}, 0)

	return nil
}

func (self *Sql) Finalize(filter filter.Filter) error {
	switch self.Type {
	case SqlSelectStatement:
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

			for i, criterionStr := range self.criteria {
				self.Push([]byte(criterionStr))

				// do this for all but the last criterion
				if i+1 < len(self.criteria) {
					self.Push([]byte(` `))
				}
			}
		}

	case SqlInsertStatement:
		if len(self.criteria) == 0 {
			return fmt.Errorf("INSERT statements must specify at least one criterion")
		}

		self.Push([]byte(`INSERT INTO `))
		self.Push([]byte(self.collection))

		self.Push([]byte(`(`))

		fieldNames := make([]string, len(self.criteriaFields))

		for i, f := range self.criteriaFields {
			fieldNames[i] = fmt.Sprintf(self.FieldNameFormat, f)
		}

		self.Push([]byte(strings.Join(fieldNames, `,`)))
		self.Push([]byte(`) VALUES (`))
		self.Push([]byte(strings.Join(self.criteriaValues, `,`)))
		self.Push([]byte(`)`))

	case SqlUpdateStatement:

	default:
		return fmt.Errorf("Unknown statement type")
	}

	return nil
}

func (self *Sql) WithField(field string) error {
	self.fields = append(self.fields, field)
	return nil
}

func (self *Sql) SetOption(key, value string) error {
	return nil
}

func (self *Sql) GetValues() []interface{} {
	return self.values
}

func (self *Sql) WithCriterion(criterion filter.Criterion) error {
	criterionStr := ``

	if len(self.criteria) == 0 {
		criterionStr = `WHERE (`
	} else {
		criterionStr = `AND (`
	}

	outValues := make([]string, 0)

	self.criteriaFields = append(self.criteriaFields, criterion.Field)

	for i, value := range criterion.Values {
		self.criteriaValues = append(self.criteriaValues, value)

		var typedValue interface{}
		var convertErr error

		switch criterion.Type {
		case `str`:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.String, value)
		case `float`:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.Float, value)
		case `int`:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.Integer, value)
		case `bool`:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.Boolean, value)
		case `date`, `time`:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.Time, value)
		default:
			typedValue = stringutil.Autotype(value)
		}

		if convertErr != nil {
			return convertErr
		}

		self.values = append(self.values, typedValue)

		if self.UsePlaceholders {
			switch self.PlaceholderArgument {
			case `index`:
				value = fmt.Sprintf(self.PlaceholderFormat, i)
			case `index1`:
				value = fmt.Sprintf(self.PlaceholderFormat, i+1)
			case `field`:
				value = fmt.Sprintf(self.PlaceholderFormat, criterion.Field)
			default:
				value = self.PlaceholderFormat
			}
		}

		outVal := ``
		var fmtstr string

		// handle quoting of string values in query statements
		switch typedValue.(type) {
		case string:
			fmtstr = self.QuotedValueFormat
		default:
			fmtstr = self.UnquotedValueFormat
		}

		if criterion.Type != `` {
			if criterionType, err := self.filterTypeToSqlType(criterion.Type, criterion.Length); err == nil {
				outVal = outVal + fmt.Sprintf("CAST(%s AS %s)", fmt.Sprintf(self.FieldNameFormat, criterion.Field), criterionType)
			} else {
				return err
			}
		} else {
			outVal = outVal + fmt.Sprintf(self.FieldNameFormat, criterion.Field)
		}

		switch criterion.Operator {
		case `is`, ``:
			if value == `null` {
				outVal = outVal + ` IS NULL`
			} else {
				outVal = outVal + fmt.Sprintf(" = "+fmtstr, value)
			}
		case `not`:
			if value == `null` {
				outVal = outVal + ` IS NOT NULL`
			} else {
				outVal = outVal + fmt.Sprintf(" <> "+fmtstr, value)
			}
		case `contains`:
			outVal = outVal + fmt.Sprintf(` LIKE '%%%%%s%%%%'`, value)
		case `prefix`:
			outVal = outVal + fmt.Sprintf(` LIKE '%s%%%%'`, value)
		case `suffix`:
			outVal = outVal + fmt.Sprintf(` LIKE '%%%%%s'`, value)
		case `gt`:
			outVal = outVal + fmt.Sprintf(" > "+fmtstr, value)
		case `gte`:
			outVal = outVal + fmt.Sprintf(" >= "+fmtstr, value)
		case `lt`:
			outVal = outVal + fmt.Sprintf(" < "+fmtstr, value)
		case `lte`:
			outVal = outVal + fmt.Sprintf(" <= "+fmtstr, value)
		default:
			return fmt.Errorf("Unimplemented operator '%s'", criterion.Operator)
		}

		outValues = append(outValues, outVal)
	}

	criterionStr = criterionStr + strings.Join(outValues, ` OR `) + `)`

	self.criteria = append(self.criteria, criterionStr)

	return nil
}

func (self *Sql) filterTypeToSqlType(in string, length int) (string, error) {
	out := ``

	switch strings.ToLower(in) {
	case `str`:
		out = self.TypeMapping.StringType
	case `int`:
		out = self.TypeMapping.IntegerType
	case `float`:
		out = self.TypeMapping.FloatType
	case `bool`:
		out = self.TypeMapping.BooleanType

		if l := self.TypeMapping.BooleanTypeLength; l > 0 {
			length = l
		}
	case `date`, `time`:
		out = self.TypeMapping.DateTimeType
	default:
		out = in
	}

	if length > 0 {
		out = out + fmt.Sprintf("(%d)", length)
	}

	return strings.ToUpper(out), nil
}
