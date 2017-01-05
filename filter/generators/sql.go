package generators

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/filter"
	"sort"
	"strings"
)

// SQL Generator

type SqlStatementType int

const (
	SqlSelectStatement SqlStatementType = iota
	SqlInsertStatement
	SqlUpdateStatement
	SqlDeleteStatement
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

var NoTypeMapping = SqlTypeMapping{}

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

var PostgresTypeMapping = SqlTypeMapping{
	StringType:   `TEXT`,
	IntegerType:  `BIGINT`,
	FloatType:    `NUMERIC`,
	BooleanType:  `BOOLEAN`,
	DateTimeType: `TIMESTAMP`,
}

var SqliteTypeMapping = SqlTypeMapping{
	StringType:        `TEXT`,
	IntegerType:       `INTEGER`,
	FloatType:         `REAL`,
	BooleanType:       `INTEGER`,
	BooleanTypeLength: 1,
	DateTimeType:      `INTEGER`,
}

var DefaultSqlTypeMapping = MysqlTypeMapping

type Sql struct {
	filter.Generator
	TableNameFormat     string
	FieldNameFormat     string
	UsePlaceholders     bool
	PlaceholderFormat   string
	PlaceholderArgument string // if specified, either "index", "index1" or "field"
	UnquotedValueFormat string
	QuotedValueFormat   string
	UseInStatement      bool
	TypeMapping         SqlTypeMapping
	Type                SqlStatementType
	InputData           map[string]interface{}
	collection          string
	fields              []string
	criteria            []string
	inputValues         []interface{}
	values              []interface{}
}

func NewSqlGenerator() *Sql {
	return &Sql{
		Generator:           filter.Generator{},
		PlaceholderFormat:   `?`,
		PlaceholderArgument: ``,
		UnquotedValueFormat: "%v",
		QuotedValueFormat:   "'%s'",
		TableNameFormat:     "%s",
		FieldNameFormat:     "%s",
		UseInStatement:      true,
		TypeMapping:         DefaultSqlTypeMapping,
		Type:                SqlSelectStatement,
		InputData:           make(map[string]interface{}),
	}
}

func (self *Sql) Initialize(collectionName string) error {
	self.Reset()
	self.collection = self.ToTableName(collectionName)
	self.fields = make([]string, 0)
	self.criteria = make([]string, 0)
	self.inputValues = make([]interface{}, 0)
	self.values = make([]interface{}, 0)

	return nil
}

func (self *Sql) Finalize(_ filter.Filter) error {
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

		self.populateWhereClause()

	case SqlInsertStatement:
		if self.InputData == nil || len(self.InputData) == 0 {
			return fmt.Errorf("INSERT statements must specify input data")
		}

		self.Push([]byte(`INSERT INTO `))
		self.Push([]byte(self.collection))

		self.Push([]byte(` (`))

		fieldNames := maputil.StringKeys(self.InputData)

		for i, f := range fieldNames {
			fieldNames[i] = self.ToFieldName(f)
		}

		sort.Strings(fieldNames)

		self.Push([]byte(strings.Join(fieldNames, `, `)))
		self.Push([]byte(`) VALUES (`))

		values := make([]string, 0)

		for i, field := range maputil.StringKeys(self.InputData) {
			v, _ := self.InputData[field]
			values = append(values, self.ToValue(field, i, v, ``))
			self.inputValues = append(self.inputValues, v)
		}

		self.Push([]byte(strings.Join(values, `, `)))
		self.Push([]byte(`)`))

	case SqlUpdateStatement:
		if self.InputData == nil || len(self.InputData) == 0 {
			return fmt.Errorf("UPDATE statements must specify input data")
		}

		self.Push([]byte(`UPDATE `))
		self.Push([]byte(self.collection))
		self.Push([]byte(` SET `))

		updatePairs := make([]string, 0)

		var i int
		fieldNames := maputil.StringKeys(self.InputData)
		sort.Strings(fieldNames)

		for _, field := range fieldNames {
			value, _ := self.InputData[field]

			// do this first because we want the unmodified field name
			vStr := self.ToValue(field, i, value, ``)
			self.inputValues = append(self.inputValues, value)

			field := self.ToFieldName(field)

			updatePairs = append(updatePairs, fmt.Sprintf("%s = %s", field, vStr))

			i += 1
		}

		self.Push([]byte(strings.Join(updatePairs, `, `)))

		self.populateWhereClause()

	case SqlDeleteStatement:
		self.Push([]byte(`DELETE FROM `))
		self.Push([]byte(self.collection))
		self.populateWhereClause()

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
	return append(self.inputValues, self.values...)
}

func (self *Sql) WithCriterion(criterion filter.Criterion) error {
	criterionStr := ``

	if len(self.criteria) == 0 {
		criterionStr = `WHERE (`
	} else {
		criterionStr = `AND (`
	}

	outValues := make([]string, 0)

	// whether to wrap is: and not: queries containing multiple values in an IN() group
	// rather than producing "f = x OR f = y OR f = x ..."
	//
	var useInStatement bool

	if self.UseInStatement {
		if len(criterion.Values) > 1 {
			switch criterion.Operator {
			case ``, `is`, `not`:
				useInStatement = true
			}
		}
	}

	outFieldName := criterion.Field

	for _, value := range criterion.Values {
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

		value = self.ToValue(criterion.Field, len(self.criteria), typedValue, criterion.Operator)

		outVal := ``

		if !useInStatement {
			if criterion.Type != `` {
				if criterionType, err := self.ToNativeType(criterion.Type, criterion.Length); err == nil {
					if criterionType != `` {
						outFieldName = fmt.Sprintf("CAST(%s AS %s)", self.ToFieldName(criterion.Field), criterionType)
						outVal = outFieldName
					}
				} else {
					return err
				}
			}

			if outVal == `` {
				outFieldName = self.ToFieldName(criterion.Field)
				outVal = outFieldName
			}
		}

		switch criterion.Operator {
		case `is`, ``:
			if value == `NULL` {
				outVal = outVal + ` IS NULL`
			} else {
				if useInStatement {
					outVal = outVal + fmt.Sprintf("%s", value)
				} else {
					outVal = outVal + fmt.Sprintf(" = %s", value)
				}
			}
		case `not`:
			if value == `NULL` {
				outVal = outVal + ` IS NOT NULL`
			} else {
				if useInStatement {
					outVal = outVal + fmt.Sprintf("%s", value)
				} else {
					outVal = outVal + fmt.Sprintf(" <> %s", value)
				}
			}
		case `contains`:
			outVal = outVal + fmt.Sprintf(` LIKE '%%%%%s%%%%'`, value)
		case `prefix`:
			outVal = outVal + fmt.Sprintf(` LIKE '%s%%%%'`, value)
		case `suffix`:
			outVal = outVal + fmt.Sprintf(` LIKE '%%%%%s'`, value)
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

	if useInStatement {
		criterionStr = criterionStr + outFieldName + ` `

		if criterion.Operator == `not` {
			criterionStr = criterionStr + `NOT `
		}

		criterionStr = criterionStr + `IN(` + strings.Join(outValues, `, `) + `))`
	} else {
		criterionStr = criterionStr + strings.Join(outValues, ` OR `) + `)`
	}

	self.criteria = append(self.criteria, criterionStr)

	return nil
}

func (self *Sql) ToTableName(table string) string {
	return fmt.Sprintf(self.TableNameFormat, table)
}

func (self *Sql) ToFieldName(field string) string {
	return fmt.Sprintf(self.FieldNameFormat, field)
}

func (self *Sql) ToNativeType(in string, length int) (string, error) {
	out := ``

	switch strings.ToLower(in) {
	case `str`:
		out = self.TypeMapping.StringType

		if l := self.TypeMapping.StringTypeLength; length == 0 && l > 0 {
			length = l
		}
	case `int`:
		out = self.TypeMapping.IntegerType
	case `float`:
		out = self.TypeMapping.FloatType
	case `bool`:
		out = self.TypeMapping.BooleanType

		if l := self.TypeMapping.BooleanTypeLength; length == 0 && l > 0 {
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

func (self *Sql) SplitTypeLength(in string) (string, int, int) {
	var length int
	var precision int

	parts := strings.SplitN(strings.ToUpper(in), `(`, 2)

	if len(parts) == 2 {
		nums := strings.SplitN(strings.TrimSuffix(parts[1], `)`), `,`, 2)

		if len(nums) == 2 {
			if v, err := stringutil.ConvertToInteger(nums[1]); err == nil {
				precision = int(v)
			}
		}

		if v, err := stringutil.ConvertToInteger(nums[0]); err == nil {
			length = int(v)
		}
	}

	return parts[0], length, precision
}

func (self *Sql) ToValue(fieldName string, fieldIndex int, in interface{}, operator string) string {
	if self.UsePlaceholders {
		switch self.PlaceholderArgument {
		case `index`:
			return fmt.Sprintf(self.PlaceholderFormat, fieldIndex)
		case `index1`:
			return fmt.Sprintf(self.PlaceholderFormat, fieldIndex+1)
		case `field`:
			return fmt.Sprintf(self.PlaceholderFormat, fieldName)
		default:
			return self.PlaceholderFormat
		}
	}

	// handle quoting of string values in query statements
	switch in.(type) {
	case nil:
		return `NULL`
	case string:
		switch in.(string) {
		case `null`:
			return `NULL`
		default:
			switch operator {
			case `prefix`, `contains`, `suffix`:
				return in.(string)
			default:
				return fmt.Sprintf(self.QuotedValueFormat, in.(string))
			}
		}
	default:
		return fmt.Sprintf(self.UnquotedValueFormat, in)
	}
}

func (self *Sql) populateWhereClause() {
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
}
