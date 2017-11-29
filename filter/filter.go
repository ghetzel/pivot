package filter

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
)

var CriteriaSeparator = `/`
var FieldTermSeparator = `/`
var FieldLengthDelimiter = `#`
var ModifierDelimiter = `:`
var ValueSeparator = `|`
var QueryUnescapeValues = false
var AllValue = `all`
var SortAscending = `+`
var SortDescending = `-`
var DefaultIdentityField = `id`

type NormalizerFunc func(in string) string // {}

var DefaultNormalizerFunc = func(in string) string {
	in = strings.ToLower(in)
	in = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return r
		}

		return -1
	}, in)

	return in
}

type Criterion struct {
	Type        dal.Type      `json:"type,omitempty"`
	Length      int           `json:"length,omitempty"`
	Field       string        `json:"field"`
	Operator    string        `json:"operator,omitempty"`
	Values      []interface{} `json:"values"`
	Aggregation Aggregation   `json:"aggregation,omitempty"`
}

type SortBy struct {
	Field      string
	Descending bool
}

type Aggregation int

const (
	First Aggregation = iota
	Last
	Minimum
	Maximum
	Sum
	Average
	Count
)

type Aggregate struct {
	Aggregation Aggregation
	Field       string
}

func (self *Criterion) String() string {
	rv := ``

	if self.Type != `` {
		if self.Length > 0 {
			rv += fmt.Sprintf("%v%s%d%s", self.Type, FieldLengthDelimiter, self.Length, ModifierDelimiter)
		} else {
			rv += fmt.Sprintf("%v%s", self.Type, ModifierDelimiter)
		}
	}

	rv += self.Field + FieldTermSeparator

	if self.Operator != `` {
		rv += self.Operator + ModifierDelimiter
	}

	values := make([]string, 0)

	for _, value := range self.Values {
		vStr := fmt.Sprintf("%v", value)

		if QueryUnescapeValues {
			values = append(values, url.QueryEscape(vStr))
		} else {
			values = append(values, vStr)
		}
	}

	rv += strings.Join(values, ValueSeparator)

	return rv
}

type Filter struct {
	Spec          string
	MatchAll      bool
	Offset        int
	Limit         int
	Criteria      []Criterion
	Sort          []string
	Fields        []string
	Options       map[string]interface{}
	Paginate      bool
	IdentityField string
	Normalizer    NormalizerFunc
}

func New() *Filter {
	return &Filter{
		Criteria:      make([]Criterion, 0),
		Sort:          make([]string, 0),
		Fields:        make([]string, 0),
		Options:       make(map[string]interface{}),
		Paginate:      true,
		IdentityField: DefaultIdentityField,
		Normalizer:    DefaultNormalizerFunc,
	}
}

func MakeFilter(specs ...string) Filter {
	spec := strings.Join(specs, CriteriaSeparator)

	f := Filter{
		Spec:          spec,
		Criteria:      make([]Criterion, 0),
		Sort:          make([]string, 0),
		Fields:        make([]string, 0),
		Options:       make(map[string]interface{}),
		Paginate:      true,
		IdentityField: DefaultIdentityField,
		Normalizer:    DefaultNormalizerFunc,
	}

	if spec == AllValue {
		f.MatchAll = true
	}

	return f
}

func Copy(other *Filter) Filter {
	return *other
}

func FromMap(in map[string]interface{}) (*Filter, error) {
	rv := MakeFilter()

	for typeField, opValue := range in {
		fType, fName := SplitModifierToken(typeField)
		var vOper string
		var vValues interface{}

		if pair, ok := opValue.(string); ok {
			vOper, vValues = SplitModifierToken(pair)
		} else {
			vValues = opValue
		}

		rv.AddCriteria(Criterion{
			Type:     dal.Type(fType),
			Field:    fName,
			Operator: vOper,
			Values:   sliceutil.Sliceify(vValues),
		})
	}

	return &rv, nil
}

func Null() *Filter {
	f := MakeFilter(``)
	return &f
}

func All() *Filter {
	f := MakeFilter(AllValue)
	return &f
}

// Filter syntax definition
//
// filter     ::= ([sort]field/value | [sort]type:field/value | [sort]type:field/comparator:value)+
// sort       ::= ASCII plus (+), minus (-)
// field      ::= ? US-ASCII field name ?;
// value      ::= ? UTF-8 field value ?;
// type       ::= str | bool | int | float | date
// comparator :=  is | not | gt | gte | lt | lte | prefix | suffix | regex
//
func Parse(spec string) (*Filter, error) {
	var criterion Criterion

	spec = strings.TrimPrefix(spec, `/`)

	rvV := MakeFilter(spec)
	rv := &rvV
	criteriaPre := strings.Split(spec, CriteriaSeparator)
	criteria := make([]string, 0)

	if CriteriaSeparator == FieldTermSeparator {
		criteria = criteriaPre
	} else {
		for _, fieldTerm := range criteriaPre {
			parts := strings.SplitN(fieldTerm, FieldTermSeparator, 2)

			criteria = append(criteria, parts...)
		}
	}

	switch {
	case spec == ``:
		nullFilter := MakeFilter(``)
		return &nullFilter, nil

	case spec == AllValue:
		return rv, nil

	case len(criteria) >= 2:
		for i, token := range criteria {
			if (i % 2) == 0 {
				var addSortAsc *bool

				if strings.HasPrefix(token, SortAscending) {
					v := true
					addSortAsc = &v
				} else if strings.HasPrefix(token, SortDescending) {
					v := false
					addSortAsc = &v
				}

				// remove sort prefixes
				token = strings.TrimPrefix(token, SortDescending)
				token = strings.TrimPrefix(token, SortAscending)

				fType, fName := SplitModifierToken(token)

				if fType == `` {
					criterion = Criterion{
						Field: fName,
						Type:  dal.AutoType,
					}
				} else {
					typeLengthPair := strings.SplitN(fType, FieldLengthDelimiter, 2)

					if len(typeLengthPair) == 1 {
						criterion = Criterion{
							Type:  sliceutil.Or(dal.Type(fType), dal.StringType).(dal.Type),
							Field: fName,
						}
					} else {
						if v, err := strconv.ParseUint(typeLengthPair[1], 10, 32); err == nil {
							criterion = Criterion{
								Type:   sliceutil.Or(dal.Type(typeLengthPair[0]), dal.StringType).(dal.Type),
								Length: int(v),
								Field:  fName,
							}
						} else {
							return rv, err
						}
					}
				}

				if addSortAsc != nil {
					if *addSortAsc == true {
						rv.Sort = append(rv.Sort, criterion.Field)
					} else {
						rv.Sort = append(rv.Sort, SortDescending+criterion.Field)
					}
				}
			} else {
				vOper, vValue := SplitModifierToken(token)
				criterion.Values = make([]interface{}, 0)

				if vOper != `` {
					criterion.Operator = vOper
				}

				for _, v := range strings.Split(vValue, ValueSeparator) {
					criterion.Values = append(criterion.Values, v)
				}

				if QueryUnescapeValues {
					for i, value := range criterion.Values {
						if v, err := url.QueryUnescape(fmt.Sprintf("%v", value)); err == nil {
							criterion.Values[i] = v
						} else {
							return rv, err
						}
					}
				}

				rv.Criteria = append(rv.Criteria, criterion)
			}
		}
	default:
		return rv, fmt.Errorf("Invalid filter spec: %s", spec)
	}

	return rv, nil
}

func MustParse(spec string) *Filter {
	if f, err := Parse(spec); err == nil {
		return f
	} else {
		panic(err.Error())
	}
}

func (self *Filter) AddCriteria(criteria ...Criterion) *Filter {
	self.Criteria = append(self.Criteria, criteria...)
	return self
}

func (self *Filter) SortBy(fields ...string) *Filter {
	if len(fields) > 0 {
		self.Sort = fields
	}

	return self
}

func (self *Filter) WithFields(fields ...string) *Filter {
	if len(fields) > 0 {
		self.Fields = append(self.Fields, fields...)
	}

	return self
}

func (self *Filter) BoundedBy(limit int, offset int) *Filter {
	if limit >= 0 {
		self.Limit = limit
	}

	if offset >= 0 {
		self.Offset = offset
	}

	return self
}

func (self *Filter) CriteriaFields() []string {
	fields := make([]string, len(self.Criteria))

	for i, criterion := range self.Criteria {
		fields[i] = criterion.Field
	}

	return fields
}

func (self *Filter) IdOnly() bool {
	if self.Fields != nil && len(self.Fields) == 1 && self.Fields[0] == self.IdentityField {
		return true
	}

	return false
}

func (self *Filter) GetValues(field string) ([]interface{}, bool) {
	for _, criterion := range self.Criteria {
		if criterion.Field == field {
			return criterion.Values, true
		}
	}

	return nil, false
}

func (self *Filter) GetFirstValue() (interface{}, bool) {
	if len(self.Criteria) > 0 {
		if len(self.Criteria[0].Values) > 0 {
			return self.Criteria[0].Values[0], true
		}
	}

	return nil, false
}

func (self *Filter) GetIdentityValue() (interface{}, bool) {
	for _, criterion := range self.Criteria {
		if criterion.Field == self.IdentityField {
			return sliceutil.At(criterion.Values, 0)
		}
	}

	return nil, false
}

func (self *Filter) IsMatchAll() bool {
	if self.MatchAll || self.Spec == AllValue {
		return true
	}

	return false
}

func (self *Filter) String() string {
	if self.MatchAll {
		return AllValue
	} else {
		criteria := make([]string, 0)

		for _, criterion := range self.Criteria {
			criteria = append(criteria, criterion.String())
		}

		return strings.Join(criteria, CriteriaSeparator)
	}
}

func (self *Filter) GetSort() []SortBy {
	sortBy := make([]SortBy, len(self.Sort))

	for i, s := range self.Sort {
		desc := strings.HasPrefix(s, SortDescending)
		s = strings.TrimPrefix(s, SortDescending)
		s = strings.TrimPrefix(s, SortAscending)

		sortBy[i] = SortBy{
			Field:      s,
			Descending: desc,
		}
	}

	return sortBy
}

func (self *Filter) ApplyOptions(in interface{}) error {
	if len(self.Options) > 0 {
		s := structs.New(in)

		for name, value := range self.Options {
			if f, ok := s.FieldOk(name); ok {
				if err := f.Set(value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (self *Filter) NewFromMap(in map[string]interface{}) (*Filter, error) {
	criteria := make([]string, 0)

	for _, criterion := range self.Criteria {
		criteria = append(criteria, criterion.String())
	}

	for typeField, opValue := range in {
		criteria = append(criteria, fmt.Sprintf("%s%s%v", typeField, FieldTermSeparator, opValue))
	}

	return Parse(strings.Join(criteria, CriteriaSeparator))
}

func (self *Filter) NewFromSpec(specs ...string) (*Filter, error) {
	criteria := make([]string, 0)

	for _, criterion := range self.Criteria {
		criteria = append(criteria, criterion.String())
	}

	criteria = append(criteria, specs...)

	return Parse(strings.Join(criteria, CriteriaSeparator))
}

func (self *Filter) MatchesRecord(record *dal.Record) bool {
	if self.IsMatchAll() {
		return true
	}

	if record == nil {
		return false
	}

	for _, criterion := range self.Criteria {
		for _, vI := range criterion.Values {
			vStr := fmt.Sprintf("%v", vI)

			// if the operator isn't of the exact match sort, normalize the criterion value
			if !IsExactMatchOperator(criterion.Operator) {
				vStr = self.Normalizer(vStr)
			}

			// treat unset criterion values and the literal value "null" as nil
			switch vStr {
			case `null`, ``:
				vI = nil
			}

			var invertQuery bool
			var cmpValue interface{}
			var cmpValueS string

			if criterion.Field == self.IdentityField {
				cmpValue = record.ID
			} else {
				cmpValue = record.Get(criterion.Field)
			}

			if cmpValue != nil {
				cmpValueS = fmt.Sprintf("%v", cmpValue)

				// if the operator isn't of the exact match sort, normalize the record field value
				if !IsExactMatchOperator(criterion.Operator) {
					cmpValueS = self.Normalizer(cmpValueS)
				}
			}

			// fmt.Printf("term:%v value:%v\n", vStr, cmpValueS)

			switch criterion.Operator {
			case `is`, ``, `not`, `like`, `unlike`:
				var isEqual bool

				invertQuery = IsInvertingOperator(criterion.Operator)

				switch criterion.Type {
				case dal.AutoType:
					if e, err := stringutil.RelaxedEqual(vStr, cmpValueS); err == nil {
						isEqual = e
					} else {
						return false
					}
				case dal.FloatType:
					if vF, err := stringutil.ConvertToFloat(vI); err == nil {
						if cF, err := stringutil.ConvertToFloat(cmpValue); err == nil {
							isEqual = (vF == cF)
						}
					}

				case dal.IntType:
					if vInt, err := stringutil.ConvertToFloat(vI); err == nil {
						if cI, err := stringutil.ConvertToFloat(cmpValue); err == nil {
							isEqual = (vInt == cI)
						}
					}

				case dal.BooleanType:
					if vBool, err := stringutil.ConvertToBool(vI); err == nil {
						if cB, err := stringutil.ConvertToBool(cmpValue); err == nil {
							isEqual = (vBool == cB)
						}
					}

				default:
					isEqual = (vI == cmpValue)
				}

				if !invertQuery && !isEqual || invertQuery && isEqual {
					return false
				}

			case `prefix`:
				if !strings.HasPrefix(cmpValueS, vStr) {
					return false
				}

			case `suffix`:
				if !strings.HasSuffix(cmpValueS, vStr) {
					return false
				}

			case `contains`:
				if !strings.Contains(cmpValueS, vStr) {
					return false
				}

			case `gt`, `lt`, `gte`, `lte`:
				var cmpValueF float64
				var vF float64

				if v, err := stringutil.ConvertToFloat(vI); err == nil {
					vF = v

					if c, err := stringutil.ConvertToFloat(cmpValue); err == nil {
						cmpValueF = c
					} else {
						return false
					}
				} else {
					return false
				}

				switch criterion.Operator {
				case `gt`:
					if !(cmpValueF > vF) {
						return false
					}
				case `gte`:
					if !(cmpValueF >= vF) {
						return false
					}
				case `lt`:
					if !(cmpValueF < vF) {
						return false
					}
				case `lte`:
					if !(cmpValueF <= vF) {
						return false
					}
				}

			default:
				return false
			}
		}
	}

	return true
}

func IsExactMatchOperator(operator string) bool {
	switch operator {
	case ``, `is`, `not`, `gt`, `gte`, `lt`, `lte`:
		return true
	}

	return false
}

func IsInvertingOperator(operator string) bool {
	switch operator {
	case `not`, `unlike`:
		return true
	}

	return false
}

func SplitModifierToken(in string) (string, string) {
	parts := strings.SplitN(in, ModifierDelimiter, 2)

	if len(parts) == 1 {
		return ``, parts[0]
	} else {
		return parts[0], parts[1]
	}
}
