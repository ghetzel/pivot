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
	"github.com/ghetzel/go-stockutil/rxutil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

var SqlMaxPlaceholders = 16384

type sqlRangeValue struct {
	lower interface{}
	upper interface{}
}

func (self sqlRangeValue) String() string {
	return fmt.Sprintf("%v:%v", self.lower, self.upper)
}

type SqlObjectTypeEncodeFunc func(in interface{}) ([]byte, error)
type SqlObjectTypeDecodeFunc func(in []byte, out interface{}) error
type SqlArrayTypeEncodeFunc func(in interface{}) ([]byte, error)
type SqlArrayTypeDecodeFunc func(in []byte, out interface{}) error

var SqlJsonTypeEncoder = func(in interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(in)
	return buf.Bytes(), err
}

var SqlJsonTypeDecoder = func(in []byte, out interface{}) error {
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
	Name                  string
	StringType            string
	StringTypeLength      int
	IntegerType           string
	FloatType             string
	FloatTypeLength       int
	FloatTypePrecision    int
	BooleanType           string
	BooleanTypeLength     int
	DateTimeType          string
	ObjectType            string
	ArrayType             string
	RawType               string
	SubtypeFormat         string
	MultiSubtypeFormat    string
	PlaceholderFormat     string                  // if using placeholders, the format string used to insert them
	PlaceholderArgument   string                  // if specified, either "index", "index1" or "field"
	TableNameFormat       string                  // format string used to wrap table names
	FieldNameFormat       string                  // format string used to wrap field names
	NestedFieldNameFormat string                  // map of field name-format strings to wrap fields addressing nested map keys. supercedes FieldNameFormat
	NestedFieldSeparator  string                  // the string used to denote nesting in a nested field name
	NestedFieldJoiner     string                  // the string used to re-join all but the first value in a nested field when interpolating into NestedFieldNameFormat
	ObjectTypeEncodeFunc  SqlObjectTypeEncodeFunc // function used for encoding objects to a native representation
	ObjectTypeDecodeFunc  SqlObjectTypeDecodeFunc // function used for decoding objects from native into a destination map
	ArrayTypeEncodeFunc   SqlArrayTypeEncodeFunc  // function used for encoding arrays to a native representation
	ArrayTypeDecodeFunc   SqlArrayTypeDecodeFunc  // function used for decoding arrays from native into a destination map
}

func (self SqlTypeMapping) String() string {
	return self.Name
}

var NoTypeMapping = SqlTypeMapping{}

var GenericTypeMapping = SqlTypeMapping{
	Name:                 `generic`,
	StringType:           `VARCHAR`,
	StringTypeLength:     255,
	IntegerType:          `BIGINT`,
	FloatType:            `DECIMAL`,
	FloatTypeLength:      10,
	FloatTypePrecision:   8,
	BooleanType:          `BOOL`,
	DateTimeType:         `DATETIME`,
	ObjectType:           `BLOB`,
	ArrayType:            `BLOB`,
	RawType:              `BLOB`,
	PlaceholderFormat:    `?`,
	PlaceholderArgument:  ``,
	TableNameFormat:      "%s",
	FieldNameFormat:      "%s",
	NestedFieldSeparator: `.`,
	NestedFieldJoiner:    `.`,
}

var CassandraTypeMapping = SqlTypeMapping{
	Name:                 `cassandra`,
	StringType:           `VARCHAR`,
	IntegerType:          `INT`,
	FloatType:            `FLOAT`,
	BooleanType:          `TINYINT`,
	BooleanTypeLength:    1,
	DateTimeType:         `DATETIME`,
	ObjectType:           `MAP`,
	ArrayType:            `LIST`,
	RawType:              `BLOB`,
	SubtypeFormat:        `%s<%v>`,
	MultiSubtypeFormat:   `%s<%v,%v>`,
	PlaceholderFormat:    `TODO`,
	PlaceholderArgument:  `TODO`,
	TableNameFormat:      "%s",
	FieldNameFormat:      "%s",
	NestedFieldSeparator: `.`,
	NestedFieldJoiner:    `.`,
}

var MysqlTypeMapping = SqlTypeMapping{
	Name:                 `mysql`,
	StringType:           `VARCHAR`,
	StringTypeLength:     255,
	IntegerType:          `BIGINT`,
	FloatType:            `DECIMAL`,
	FloatTypeLength:      10,
	FloatTypePrecision:   8,
	BooleanType:          `BOOL`,
	DateTimeType:         `DATETIME`,
	ObjectType:           `MEDIUMBLOB`,
	ArrayType:            `MEDIUMBLOB`,
	RawType:              `MEDIUMBLOB`,
	PlaceholderFormat:    `?`,
	PlaceholderArgument:  ``,
	TableNameFormat:      "`%s`",
	FieldNameFormat:      "`%s`",
	NestedFieldSeparator: `.`,
	NestedFieldJoiner:    `.`,
}

var PostgresTypeMapping = SqlTypeMapping{
	Name:                 `postgres`,
	StringType:           `TEXT`,
	IntegerType:          `BIGINT`,
	FloatType:            `NUMERIC`,
	BooleanType:          `BOOLEAN`,
	DateTimeType:         `TIMESTAMP`,
	ObjectType:           `VARCHAR`,
	ArrayType:            `VARCHAR`,
	RawType:              `BYTEA`,
	PlaceholderFormat:    `$%d`,
	PlaceholderArgument:  `index1`,
	TableNameFormat:      "%q",
	FieldNameFormat:      "%q",
	NestedFieldSeparator: `.`,
	NestedFieldJoiner:    `.`,
}

var PostgresJsonTypeMapping = SqlTypeMapping{
	Name:         `postgres-json`,
	StringType:   `TEXT`,
	IntegerType:  `BIGINT`,
	FloatType:    `NUMERIC`,
	BooleanType:  `BOOLEAN`,
	DateTimeType: `TIMESTAMP`,
	// ObjectType:   `JSONB`, // TODO: implement the JSONB functionality in PostgreSQL 9.2+
	ObjectType:           `VARCHAR`,
	ArrayType:            `VARCHAR`,
	RawType:              `BYTEA`,
	PlaceholderFormat:    `$%d`,
	PlaceholderArgument:  `index1`,
	TableNameFormat:      "%q",
	FieldNameFormat:      "%q",
	NestedFieldSeparator: `.`,
	NestedFieldJoiner:    `.`,
}

var SqliteTypeMapping = SqlTypeMapping{
	Name:                 `sqlite`,
	StringType:           `TEXT`,
	IntegerType:          `INTEGER`,
	FloatType:            `REAL`,
	BooleanType:          `INTEGER`,
	BooleanTypeLength:    1,
	DateTimeType:         `INTEGER`,
	ObjectType:           `BLOB`,
	ArrayType:            `BLOB`,
	RawType:              `BLOB`,
	PlaceholderFormat:    `?`,
	PlaceholderArgument:  ``,
	TableNameFormat:      "%q",
	FieldNameFormat:      "%q",
	NestedFieldSeparator: `.`,
	NestedFieldJoiner:    `.`,
}

var DefaultSqlTypeMapping = GenericTypeMapping

func GetSqlTypeMapping(name string) (SqlTypeMapping, error) {
	switch name {
	case `postgresql`, `pgsql`:
		return PostgresTypeMapping, nil
	case `postgresql-json`, `pgsql-json`:
		return PostgresJsonTypeMapping, nil
	case `sqlite`:
		return SqliteTypeMapping, nil
	case `mysql`:
		return MysqlTypeMapping, nil
	case `cassandra`:
		return CassandraTypeMapping, nil
	case ``:
		return DefaultSqlTypeMapping, nil
	default:
		return SqlTypeMapping{}, fmt.Errorf("unrecognized SQL mapping type %q", name)
	}
}

type Sql struct {
	filter.Generator
	FieldWrappers    map[string]string      // map of field name-format strings to wrap specific fields in after FieldNameFormat is applied
	NormalizeFields  []string               // a list of field names that should have the NormalizerFormat applied to them and their corresponding values
	NormalizerFormat string                 // format string used to wrap fields and value clauses for the purpose of doing fuzzy searches
	UseInStatement   bool                   // whether multiple values in a criterion should be tested using an IN() statement
	Distinct         bool                   // whether a DISTINCT clause should be used in SELECT statements
	Count            bool                   // whether this query is being used to count rows, which means that SELECT fields are discarded in favor of COUNT(1)
	TypeMapping      SqlTypeMapping         // provides mapping information between DAL types and native SQL types
	Type             SqlStatementType       // what type of SQL statement is being generated
	InputData        map[string]interface{} // key-value data for statement types that require input data (e.g.: inserts, updates)
	collection       string
	fields           []string
	criteria         []string
	inputValues      []interface{}
	values           []interface{}
	groupBy          []string
	aggregateBy      []filter.Aggregate
	conjunction      filter.ConjunctionType
	placeholderIndex int
}

func NewSqlGenerator() *Sql {
	return &Sql{
		Generator:        filter.Generator{},
		NormalizeFields:  make([]string, 0),
		NormalizerFormat: "%s",
		FieldWrappers:    make(map[string]string),
		UseInStatement:   true,
		TypeMapping:      DefaultSqlTypeMapping,
		Type:             SqlSelectStatement,
		InputData:        make(map[string]interface{}),
	}
}

func (self *Sql) Initialize(collectionName string) error {
	self.Reset()
	self.placeholderIndex = 0
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

	defer func() {
		self.placeholderIndex = 0
	}()

	inputValues, err := self.populateInputValues()

	if err != nil {
		return err
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

					if strings.Contains(f, self.TypeMapping.NestedFieldSeparator) {
						fName = fmt.Sprintf("%v AS "+self.TypeMapping.FieldNameFormat, fName, f)
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
					fName = fmt.Sprintf("%v AS "+self.TypeMapping.FieldNameFormat, fName, aggpair.Field)
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

		self.Push([]byte(strings.Join(inputValues, `, `)))
		self.Push([]byte(`)`))
	case SqlUpdateStatement:
		if len(self.InputData) == 0 {
			return fmt.Errorf("UPDATE statements must specify input data")
		}

		self.Push([]byte(`UPDATE `))
		self.Push([]byte(self.collection))
		self.Push([]byte(` SET `))

		updatePairs := make([]string, 0)

		fieldNames := maputil.StringKeys(self.InputData)
		sort.Strings(fieldNames)

		for _, field := range fieldNames {
			field := self.ToFieldName(field)
			updatePairs = append(updatePairs, fmt.Sprintf("%s = \u2983%s\u2984", field, field))
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

	self.applyPlaceholders()

	return nil
}

func (self *Sql) populateInputValues() ([]string, error) {
	values := make([]string, 0)

	for _, field := range maputil.StringKeys(self.InputData) {
		v, _ := self.InputData[field]
		values = append(values, fmt.Sprintf("\u2983%s\u2984", field))

		if vv, err := self.PrepareInputValue(field, v); err == nil {
			self.inputValues = append(self.inputValues, vv)
		} else {
			return nil, err
		}
	}

	return values, nil
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

// Okay...so.
//
// Given the tricky, tricky nature of how values are accumulated vs. how they are exposed to
// UPDATE queries (kinda backwards), we have this placeholder system.
//
// Anywhere in the SQL query where a user input value would appear, the sequence ⦃field⦄ appears
// (note the use of Unicode characters U+2983 and U+2984 surrounding the field name.)
//
// This function goes through the final payload just before it's finalized and replaces these
// sequences with the syntax-appropriate placeholders for that field.  This ensures that the
// placeholder order matches the value order (for syntaxes that use numeric placeholders;
// e.g. PostgreSQL).
//
func (self *Sql) applyPlaceholders() {
	payload := string(self.Payload())

	for i := 0; i < SqlMaxPlaceholders; i++ {
		if match := rxutil.Match("(?P<field>\xe2\xa6\x83[^\xe2\xa6\x84]+\xe2\xa6\x84)", payload); match != nil {
			field := match.Group(`field`)
			field = strings.TrimPrefix(field, "\u2983")
			field = strings.TrimSuffix(field, "\u2984")

			payload = match.ReplaceGroup(`field`, self.GetPlaceholder(field))
		} else {
			break
		}
	}

	self.Set([]byte(payload))
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
				if typedValue == nil {
					value = `NULL`
				} else if rangepair, ok := vI.(sqlRangeValue); ok {
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
					value = fmt.Sprintf("\u2983%s\u2984", criterion.Field)
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
	return fmt.Sprintf(self.TypeMapping.TableNameFormat, table)
}

func (self *Sql) ToFieldName(field string) string {
	var formattedField string

	if field != `` {
		if nestFmt := self.TypeMapping.NestedFieldNameFormat; nestFmt != `` {
			if parts := strings.Split(field, self.TypeMapping.NestedFieldSeparator); len(parts) > 1 {
				formattedField = fmt.Sprintf(nestFmt, parts[0], strings.Join(parts[1:], self.TypeMapping.NestedFieldJoiner))
			}
		}

		if formattedField == `` {
			formattedField = fmt.Sprintf(self.TypeMapping.FieldNameFormat, field)
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
	if in == nil {
		return `NULL`
	}

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

	case dal.IntType:
		return fmt.Sprintf("%d", typeutil.Int(in))

	case dal.FloatType:
		return fmt.Sprintf("%f", typeutil.Float(in))

	default:
		return fmt.Sprintf("'%v'", in)
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

		if length > 1 {
			length = 1
		}

		if l := self.TypeMapping.BooleanTypeLength; length == 0 && l > 0 {
			length = l
		}
	case dal.TimeType:
		out = self.TypeMapping.DateTimeType

	case dal.ObjectType:
		if f := self.TypeMapping.MultiSubtypeFormat; f == `` {
			out = self.TypeMapping.ObjectType
		} else if len(subtypes) == 2 {
			if keyType, err := self.ToNativeType(subtypes[0], nil, 0); err == nil {
				if valType, err := self.ToNativeType(subtypes[1], nil, 0); err == nil {
					out = fmt.Sprintf(
						self.TypeMapping.MultiSubtypeFormat,
						self.TypeMapping.ObjectType,
						keyType,
						valType,
					)
				} else {
					return ``, err
				}
			} else {
				return ``, err
			}
		}

	case dal.ArrayType:
		if f := self.TypeMapping.SubtypeFormat; f == `` {
			out = self.TypeMapping.ArrayType
		} else if len(subtypes) == 1 {
			if valType, err := self.ToNativeType(subtypes[0], nil, 0); err == nil {
				out = fmt.Sprintf(
					self.TypeMapping.SubtypeFormat,
					self.TypeMapping.ArrayType,
					valType,
				)
			} else {
				return ``, err
			}
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

func (self *Sql) GetPlaceholder(fieldName string) string {
	// support various styles of placeholder
	// e.g.: ?, $0, $1, :fieldname
	var placeholder string

	switch self.TypeMapping.PlaceholderArgument {
	case `index`:
		placeholder = fmt.Sprintf(self.TypeMapping.PlaceholderFormat, self.placeholderIndex)
	case `index1`:
		placeholder = fmt.Sprintf(self.TypeMapping.PlaceholderFormat, self.placeholderIndex+1)
	case `field`:
		placeholder = fmt.Sprintf(self.TypeMapping.PlaceholderFormat, fieldName)
	default:
		placeholder = self.TypeMapping.PlaceholderFormat
	}

	self.placeholderIndex += 1
	return placeholder
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
		return SqlJsonTypeEncoder(value)
	default:
		return value, nil
	}
}

func (self *Sql) ObjectTypeEncode(in interface{}) ([]byte, error) {
	if !typeutil.IsMap(typeutil.ResolveValue(in)) {
		return nil, fmt.Errorf("Can only encode pointer to a map type")
	}

	if fn := self.TypeMapping.ObjectTypeEncodeFunc; fn != nil {
		return fn(in)
	} else {
		return SqlJsonTypeEncoder(in)
	}
}

func (self *Sql) ObjectTypeDecode(in []byte, out interface{}) error {
	if !typeutil.IsMap(typeutil.ResolveValue(out)) {
		return fmt.Errorf("Can only decode to pointer to a map type")
	}

	if fn := self.TypeMapping.ObjectTypeDecodeFunc; fn != nil {
		return fn(in, out)
	} else {
		return SqlJsonTypeDecoder(in, out)
	}
}

func (self *Sql) ArrayTypeEncode(in interface{}) ([]byte, error) {
	if !typeutil.IsArray(typeutil.ResolveValue(in)) {
		return nil, fmt.Errorf("Can only encode arrays")
	}

	if fn := self.TypeMapping.ArrayTypeEncodeFunc; fn != nil {
		return fn(in)
	} else {
		return SqlJsonTypeEncoder(in)
	}
}

func (self *Sql) ArrayTypeDecode(in []byte, out interface{}) error {
	if !typeutil.IsArray(typeutil.ResolveValue(out)) {
		return fmt.Errorf("Can only decode into an array")
	}

	if fn := self.TypeMapping.ArrayTypeDecodeFunc; fn != nil {
		return fn(in, out)
	} else {
		return SqlJsonTypeDecoder(in, out)
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
			typedValue, convertErr = self.ObjectTypeEncode(str)
		case dal.ArrayType:
			typedValue, convertErr = self.ArrayTypeEncode(str)
		default:
			typedValue = stringutil.Autotype(value)
		}

		if convertErr != nil {
			return nil, convertErr
		}
	}

	return typedValue, nil
}
