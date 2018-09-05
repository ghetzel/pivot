package generators

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type sqlRangeValue struct {
	lower interface{}
	upper interface{}
}

func (self sqlRangeValue) String() string {
	return fmt.Sprintf("%v:%v", self.lower, self.upper)
}

var SqlObjectTypeEncode = func(in interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(in)
	return buf.Bytes(), err
}

var SqlObjectTypeDecode = func(in []byte, out interface{}) error {
	return json.NewDecoder(bytes.NewReader(in)).Decode(out)
}

// SQL Generator

type SqlStatementType int

const (
	SqlSelectStatement SqlStatementType = iota
	SqlInsertStatement
	SqlUpdateStatement
	SqlDeleteStatement
)

type SqlTypeMapping struct {
	StringType         string
	StringTypeLength   int
	IntegerType        string
	FloatType          string
	FloatTypeLength    int
	FloatTypePrecision int
	BooleanType        string
	BooleanTypeLength  int
	DateTimeType       string
	ObjectType         string
	RawType            string
	SubtypeFormat      string
	MultiSubtypeFormat string
}

var NoTypeMapping = SqlTypeMapping{}

var CassandraTypeMapping = SqlTypeMapping{
	StringType:         `VARCHAR`,
	IntegerType:        `INT`,
	FloatType:          `FLOAT`,
	BooleanType:        `TINYINT`,
	BooleanTypeLength:  1,
	DateTimeType:       `DATETIME`,
	ObjectType:         `MAP`,
	RawType:            `BLOB`,
	SubtypeFormat:      `%s<%v>`,
	MultiSubtypeFormat: `%s<%v,%v>`,
}

var MysqlTypeMapping = SqlTypeMapping{
	StringType:         `VARCHAR`,
	StringTypeLength:   255,
	IntegerType:        `BIGINT`,
	FloatType:          `DECIMAL`,
	FloatTypeLength:    10,
	FloatTypePrecision: 8,
	BooleanType:        `BOOL`,
	DateTimeType:       `DATETIME`,
	ObjectType:         `BLOB`,
	RawType:            `BLOB`,
}

var PostgresTypeMapping = SqlTypeMapping{
	StringType:   `TEXT`,
	IntegerType:  `BIGINT`,
	FloatType:    `NUMERIC`,
	BooleanType:  `BOOLEAN`,
	DateTimeType: `TIMESTAMP`,
	ObjectType:   `BLOB`,
	RawType:      `BLOB`,
}

var PostgresJsonTypeMapping = SqlTypeMapping{
	StringType:   `TEXT`,
	IntegerType:  `BIGINT`,
	FloatType:    `NUMERIC`,
	BooleanType:  `BOOLEAN`,
	DateTimeType: `TIMESTAMP`,
	// ObjectType:   `JSONB`, // TODO: implement the JSONB functionality in PostgreSQL 9.2+
	ObjectType: `BLOB`,
	RawType:    `BLOB`,
}

var SqliteTypeMapping = SqlTypeMapping{
	StringType:        `TEXT`,
	IntegerType:       `INTEGER`,
	FloatType:         `REAL`,
	BooleanType:       `INTEGER`,
	BooleanTypeLength: 1,
	DateTimeType:      `INTEGER`,
	ObjectType:        `BLOB`,
	RawType:           `BLOB`,
}

var DefaultSqlTypeMapping = MysqlTypeMapping

type Sql struct {
	filter.Generator
	TableNameFormat       string                 // format string used to wrap table names
	FieldNameFormat       string                 // format string used to wrap field names
	NestedFieldNameFormat string                 // map of field name-format strings to wrap fields addressing nested map keys. supercedes FieldNameFormat
	NestedFieldSeparator  string                 // the string used to denote nesting in a nested field name
	NestedFieldJoiner     string                 // the string used to re-join all but the first value in a nested field when interpolating into NestedFieldNameFormat
	FieldWrappers         map[string]string      // map of field name-format strings to wrap specific fields in after FieldNameFormat is applied
	PlaceholderFormat     string                 // if using placeholders, the format string used to insert them
	PlaceholderArgument   string                 // if specified, either "index", "index1" or "field"
	NormalizeFields       []string               // a list of field names that should have the NormalizerFormat applied to them and their corresponding values
	NormalizerFormat      string                 // format string used to wrap fields and value clauses for the purpose of doing fuzzy searches
	UseInStatement        bool                   // whether multiple values in a criterion should be tested using an IN() statement
	Distinct              bool                   // whether a DISTINCT clause should be used in SELECT statements
	Count                 bool                   // whether this query is being used to count rows, which means that SELECT fields are discarded in favor of COUNT(1)
	TypeMapping           SqlTypeMapping         // provides mapping information between DAL types and native SQL types
	Type                  SqlStatementType       // what type of SQL statement is being generated
	InputData             map[string]interface{} // key-value data for statement types that require input data (e.g.: inserts, updates)
	collection            string
	fields                []string
	criteria              []string
	inputValues           []interface{}
	values                []interface{}
	groupBy               []string
	aggregateBy           []filter.Aggregate
	conjunction           filter.ConjunctionType
}

func NewSqlGenerator() *Sql {
	return &Sql{
		Generator:            filter.Generator{},
		PlaceholderFormat:    `?`,
		PlaceholderArgument:  ``,
		NormalizeFields:      make([]string, 0),
		NormalizerFormat:     "%s",
		TableNameFormat:      "%s",
		FieldNameFormat:      "%s",
		NestedFieldSeparator: `.`,
		NestedFieldJoiner:    `.`,
		FieldWrappers:        make(map[string]string),
		UseInStatement:       true,
		TypeMapping:          DefaultSqlTypeMapping,
		Type:                 SqlSelectStatement,
		InputData:            make(map[string]interface{}),
	}
}

func (self *Sql) Initialize(collectionName string) error {
	self.Reset()
	self.collection = self.ToTableName(collectionName)
	self.fields = make([]string, 0)
	self.criteria = make([]string, 0)
	self.inputValues = make([]interface{}, 0)
	self.values = make([]interface{}, 0)
	self.conjunction = filter.AndConjunction

	return nil
}

// Takes all the information collected so far and generates a SQL statement from it
func (self *Sql) Finalize(f *filter.Filter) error {
	if f != nil {
		self.conjunction = f.Conjunction
	}

	switch self.Type {
	case SqlSelectStatement:
		self.Push([]byte(`SELECT `))

		if self.Count {
			self.Push([]byte(`COUNT(1) `))
		} else {
			if self.Distinct {
				self.Push([]byte(`DISTINCT `))
			}

			if len(self.fields) == 0 && len(self.groupBy) == 0 && len(self.aggregateBy) == 0 {
				self.Push([]byte(`*`))
			} else {
				fieldNames := make([]string, 0)

				for _, f := range self.fields {
					fName := self.ToFieldName(f)

					if strings.Contains(f, self.NestedFieldSeparator) {
						fName = fmt.Sprintf("%v AS "+self.FieldNameFormat, fName, f)
					}

					fieldNames = append(fieldNames, fName)
				}

				// add the fields we're grouping by if they weren't already explicitly added by the filter
				for _, groupBy := range self.groupBy {
					if !sliceutil.ContainsString(fieldNames, groupBy) {
						fieldNames = append(fieldNames, groupBy)
					}
				}

				for _, aggpair := range self.aggregateBy {
					fName := self.ToAggregatedFieldName(aggpair.Aggregation, aggpair.Field)
					fName = fmt.Sprintf("%v AS "+self.FieldNameFormat, fName, aggpair.Field)
					fieldNames = append(fieldNames, fName)
				}

				self.Push([]byte(strings.Join(fieldNames, `, `)))
			}
		}

		self.Push([]byte(` FROM `))
		self.Push([]byte(self.collection))

		self.populateWhereClause()
		self.populateGroupBy()

		if !self.Count {
			self.populateOrderBy(f)
			self.populateLimitOffset(f)
		}

	case SqlInsertStatement:
		if len(self.InputData) == 0 {
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
			values = append(values, self.GetPlaceholder(field, i))

			if vv, err := self.PrepareInputValue(field, v); err == nil {
				self.inputValues = append(self.inputValues, vv)
			} else {
				return err
			}
		}

		self.Push([]byte(strings.Join(values, `, `)))
		self.Push([]byte(`)`))

	case SqlUpdateStatement:
		if len(self.InputData) == 0 {
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
			if vv, err := self.PrepareInputValue(field, value); err == nil {
				self.inputValues = append(self.inputValues, vv)
			} else {
				return err
			}

			field := self.ToFieldName(field)
			updatePairs = append(updatePairs, fmt.Sprintf("%s = %s", field, self.GetPlaceholder(field, i)))

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

func (self *Sql) SetOption(_ string, _ interface{}) error {
	return nil
}

func (self *Sql) GroupByField(field string) error {
	self.groupBy = append(self.groupBy, field)
	return nil
}

func (self *Sql) AggregateByField(agg filter.Aggregation, field string) error {
	self.aggregateBy = append(self.aggregateBy, filter.Aggregate{
		Aggregation: agg,
		Field:       field,
	})

	return nil
}

func (self *Sql) GetValues() []interface{} {
	return append(self.inputValues, self.values...)
}

func (self *Sql) WithCriterion(criterion filter.Criterion) error {
	criterionStr := ``

	if len(self.criteria) == 0 {
		criterionStr = `WHERE (`
	} else if self.conjunction == filter.OrConjunction {
		criterionStr = `OR (`
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
			case ``, `is`, `not`, `like`, `unlike`:
				useInStatement = true
			}
		}
	}

	outFieldName := criterion.Field

	// for multi-valued IN-statements, we need to wrap the field name in the normalizer here
	if useInStatement {
		switch criterion.Operator {
		case `like`, `unlike`:
			outFieldName = self.ApplyNormalizer(criterion.Field, outFieldName)
		}
	}

	// range queries are particular about the number of values
	if criterion.Operator == `range` {
		if len(criterion.Values) != 2 {
			return fmt.Errorf("The 'range' operator must be given exactly two values")
		}

		if lowerValue, err := self.valueToNativeRepresentation(criterion.Type, criterion.Values[0]); err == nil {
			if upperValue, err := self.valueToNativeRepresentation(criterion.Type, criterion.Values[1]); err == nil {
				criterion.Values = []interface{}{
					sqlRangeValue{
						lower: lowerValue,
						upper: upperValue,
					},
				}
			} else {
				return fmt.Errorf("invalid range upper bound: %v", err)
			}
		} else {
			return fmt.Errorf("invalid range lower bound: %v", err)
		}

	}

	// for each value being tested in this criterion
	for _, vI := range criterion.Values {
		if value, err := stringutil.ToString(vI); err == nil {
			if typedValue, err := self.valueToNativeRepresentation(criterion.Type, vI); err == nil {
				if rangepair, ok := vI.(sqlRangeValue); ok {
					typedValue = rangepair
					self.values = append(self.values, rangepair.lower)
					self.values = append(self.values, rangepair.upper)
				} else {
					// these operators use a LIKE statement, so we need to add in the right LIKE syntax
					switch criterion.Operator {
					case `prefix`:
						typedValue = fmt.Sprintf("%v", typedValue) + `%%`
					case `contains`:
						typedValue = `%%` + fmt.Sprintf("%v", typedValue) + `%%`
					case `suffix`:
						typedValue = `%%` + fmt.Sprintf("%v", typedValue)
					}

					self.values = append(self.values, typedValue)
				}

				// get the syntax-appropriate representation of the value, wrapped in normalization functions
				// if this field is (or should be treated as) a string.
				switch strings.ToUpper(value) {
				case `NULL`:
					value = strings.ToUpper(value)
				default:
					value = self.GetPlaceholder(criterion.Field, len(self.criteria))
				}

				outVal := ``

				if !useInStatement {
					outFieldName = self.ToFieldName(criterion.Field)
					outVal = outFieldName
				}

				switch criterion.Operator {
				case `is`, ``, `like`:
					if value == `NULL` {
						outVal = outVal + ` IS NULL`
					} else {

						if useInStatement {
							if criterion.Operator == `like` {
								outVal = outVal + self.ApplyNormalizer(criterion.Field, fmt.Sprintf("%s", value))
							} else {
								outVal = outVal + fmt.Sprintf("%s", value)
							}
						} else {
							if criterion.Operator == `like` {
								outVal = self.ApplyNormalizer(criterion.Field, outVal)
								outVal = outVal + fmt.Sprintf(" = %s", self.ApplyNormalizer(criterion.Field, value))
							} else {
								outVal = outVal + fmt.Sprintf(" = %s", value)
							}
						}
					}
				case `not`, `unlike`:
					if value == `NULL` {
						outVal = outVal + ` IS NOT NULL`
					} else {
						if useInStatement {
							if criterion.Operator == `unlike` {
								outVal = outVal + self.ApplyNormalizer(criterion.Field, fmt.Sprintf("%s", value))
							} else {
								outVal = outVal + fmt.Sprintf("%s", value)
							}
						} else {
							if criterion.Operator == `unlike` {
								outVal = self.ApplyNormalizer(criterion.Field, outVal)
								outVal = outVal + fmt.Sprintf(" <> %s", self.ApplyNormalizer(criterion.Field, value))
							} else {
								outVal = outVal + fmt.Sprintf(" <> %s", value)
							}
						}
					}
				case `contains`, `prefix`, `suffix`:
					// wrap the field in any string normalizing functions (the same thing
					// will happen to the values being compared)
					outVal = self.ApplyNormalizer(criterion.Field, outVal) + fmt.Sprintf(` LIKE %s`, self.ApplyNormalizer(criterion.Field, value))

				case `gt`:
					outVal = outVal + fmt.Sprintf(" > %s", value)
				case `gte`:
					outVal = outVal + fmt.Sprintf(" >= %s", value)
				case `lt`:
					outVal = outVal + fmt.Sprintf(" < %s", value)
				case `lte`:
					outVal = outVal + fmt.Sprintf(" <= %s", value)
				case `range`:
					if _, ok := typedValue.(sqlRangeValue); ok {
						outVal = outVal + fmt.Sprintf(
							" BETWEEN %v AND %v",
							self.ApplyNormalizer(criterion.Field, value),
							self.ApplyNormalizer(criterion.Field, value),
						)
					} else {
						return fmt.Errorf("Invalid value for 'range' operator")
					}
				default:
					return fmt.Errorf("Unimplemented operator '%s'", criterion.Operator)
				}

				outValues = append(outValues, outVal)
			} else {
				return err
			}
		} else {
			return err
		}
	}

	if useInStatement {
		criterionStr = criterionStr + outFieldName + ` `

		if criterion.Operator == `not` || criterion.Operator == `unlike` {
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
	var formattedField string

	if field != `` {
		if nestFmt := self.NestedFieldNameFormat; nestFmt != `` {
			if parts := strings.Split(field, self.NestedFieldSeparator); len(parts) > 1 {
				formattedField = fmt.Sprintf(nestFmt, parts[0], strings.Join(parts[1:], self.NestedFieldJoiner))
			}
		}

		if formattedField == `` {
			formattedField = fmt.Sprintf(self.FieldNameFormat, field)
		}
	}

	if wrapper, ok := self.FieldWrappers[field]; ok {
		formattedField = fmt.Sprintf(wrapper, formattedField)
	}

	return formattedField
}

func (self *Sql) ToAggregatedFieldName(agg filter.Aggregation, field string) string {
	field = self.ToFieldName(field)

	switch agg {
	case filter.First:
		return fmt.Sprintf("FIRST(%v)", field)
	case filter.Last:
		return fmt.Sprintf("LAST(%v)", field)
	case filter.Minimum:
		return fmt.Sprintf("MIN(%v)", field)
	case filter.Maximum:
		return fmt.Sprintf("MAX(%v)", field)
	case filter.Sum:
		return fmt.Sprintf("SUM(%v)", field)
	case filter.Average:
		return fmt.Sprintf("AVG(%v)", field)
	case filter.Count:
		return fmt.Sprintf("COUNT(%v)", field)
	default:
		return field
	}
}

func (self *Sql) ToNativeValue(t dal.Type, subtypes []dal.Type, in interface{}) string {
	switch t {
	case dal.StringType:
		return fmt.Sprintf("'%v'", in)
	case dal.BooleanType:
		if v, ok := in.(bool); ok {
			if v {
				return `TRUE`
			}
		}

		return `FALSE`

	// case dal.TimeType:
	// handle now/current_timestamp junk

	default:
		return fmt.Sprintf("%v", in)
	}
}

func (self *Sql) ToNativeType(in dal.Type, subtypes []dal.Type, length int) (string, error) {
	out := ``
	precision := 0

	switch in {
	case dal.StringType:
		out = self.TypeMapping.StringType

		if l := self.TypeMapping.StringTypeLength; length == 0 && l > 0 {
			length = l
		}
	case dal.IntType:
		out = self.TypeMapping.IntegerType
	case dal.FloatType:
		out = self.TypeMapping.FloatType

		if l := self.TypeMapping.FloatTypeLength; length == 0 && l > 0 {
			length = l
		}

		if p := self.TypeMapping.FloatTypePrecision; p > 0 {
			precision = p
		}
	case dal.BooleanType:
		out = self.TypeMapping.BooleanType

		if l := self.TypeMapping.BooleanTypeLength; length == 0 && l > 0 {
			length = l
		}
	case dal.TimeType:
		out = self.TypeMapping.DateTimeType

	case dal.ObjectType:
		if f := self.TypeMapping.MultiSubtypeFormat; f == `` {
			out = self.TypeMapping.ObjectType
		} else if len(subtypes) == 2 {
			out = fmt.Sprintf(
				self.TypeMapping.MultiSubtypeFormat,
				self.TypeMapping.ObjectType,
				subtypes[1],
				subtypes[2],
			)
		}

	case dal.RawType:
		out = self.TypeMapping.RawType

	default:
		out = strings.ToUpper(in.String())
	}

	if length > 0 {
		if precision > 0 {
			out = out + fmt.Sprintf("(%d,%d)", length, precision)
		} else {
			out = out + fmt.Sprintf("(%d)", length)
		}
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

func (self *Sql) GetPlaceholder(fieldName string, fieldIndex int) string {
	// support various styles of placeholder
	// e.g.: ?, $0, $1, :fieldname
	//
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

func (self *Sql) ApplyNormalizer(fieldName string, in string) string {
	if sliceutil.ContainsString(self.NormalizeFields, fieldName) {
		return fmt.Sprintf(self.NormalizerFormat, in)
	} else {
		return in
	}
}

func (self *Sql) PrepareInputValue(f string, value interface{}) (interface{}, error) {
	// times get returned as-is
	if _, ok := value.(time.Time); ok {
		return value, nil
	}

	switch reflect.ValueOf(value).Kind() {
	case reflect.Struct, reflect.Map, reflect.Ptr, reflect.Array, reflect.Slice:
		return SqlObjectTypeEncode(value)
	default:
		return value, nil
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

func (self *Sql) populateGroupBy() {
	if len(self.groupBy) > 0 {
		self.Push([]byte(` GROUP BY `))

		self.Push([]byte(strings.Join(
			sliceutil.MapString(self.groupBy, func(_ int, v string) string {
				return self.ToFieldName(v)
			}), `, `),
		))
	}
}

func (self *Sql) populateOrderBy(f *filter.Filter) {
	if sortFields := sliceutil.CompactString(f.Sort); len(sortFields) > 0 {
		self.Push([]byte(` ORDER BY `))
		orderByFields := make([]string, len(sortFields))

		for i, sortBy := range f.GetSort() {
			v := self.ToFieldName(sortBy.Field)

			if !sortBy.Descending {
				v += ` ASC`
			} else {
				v += ` DESC`
			}

			orderByFields[i] = v
		}

		self.Push([]byte(strings.Join(orderByFields, `, `)))
	}
}

func (self *Sql) populateLimitOffset(f *filter.Filter) {
	if f.Limit > 0 {
		self.Push([]byte(fmt.Sprintf(" LIMIT %d", f.Limit)))

		if f.Offset > 0 {
			self.Push([]byte(fmt.Sprintf(" OFFSET %d", f.Offset)))
		}
	}
}

func (self *Sql) valueToNativeRepresentation(coerce dal.Type, value interface{}) (interface{}, error) {
	var typedValue interface{}

	str := fmt.Sprintf("%v", value)

	// convert the value string into the appropriate language-native type
	if value == nil || strings.ToUpper(str) == `NULL` {
		str = strings.ToUpper(str)
		typedValue = nil

	} else {
		var convertErr error

		// type conversion/normalization for values extracted from the criterion
		switch coerce {
		case dal.StringType:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.String, str)
		case dal.FloatType:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.Float, str)
		case dal.IntType:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.Integer, str)
		case dal.BooleanType:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.Boolean, str)
		case dal.TimeType:
			typedValue, convertErr = stringutil.ConvertTo(stringutil.Time, str)
		case dal.ObjectType:
			typedValue, convertErr = SqlObjectTypeEncode(str)
		default:
			typedValue = stringutil.Autotype(value)
		}

		if convertErr != nil {
			return nil, convertErr
		}
	}

	return typedValue, nil
}
