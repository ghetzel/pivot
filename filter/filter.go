package filter

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/timeutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/util"
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
var rxCharFilter = regexp.MustCompile(`[\W\s\_]+`)

type NormalizerFunc func(in string) string // {}

var DefaultNormalizerFunc = func(in string) string {
	in = strings.ToLower(in)
	return rxCharFilter.ReplaceAllString(in, ``)
}

type Criterion struct {
	Type        dal.Type      `json:"type,omitempty"`
	Length      int           `json:"length,omitempty"`
	Field       string        `json:"field"`
	Operator    string        `json:"operator,omitempty"`
	Values      []interface{} `json:"values"`
	Aggregation Aggregation   `json:"aggregation,omitempty"`
}

func (self *Criterion) IsExactMatch() bool {
	switch self.Operator {
	case `is`, ``:
		return true
	default:
		return false
	}
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

type ConjunctionType string

const (
	AndConjunction ConjunctionType = ``
	OrConjunction                  = `or`
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
	Normalizer    NormalizerFunc `json:"-" bson:"-" pivot:"-"`
	Conjunction   ConjunctionType
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

		// handle the case where multiple|values|are|given|like|this
		if typeutil.IsScalar(vValues) {
			if vS := typeutil.String(vValues); strings.Contains(vS, ValueSeparator) {
				vValues = strings.Split(vS, ValueSeparator)
			}
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

func Parse(in interface{}) (*Filter, error) {
	if f, ok := in.(Filter); ok {
		return &f, nil
	} else if f, ok := in.(*Filter); ok {
		return f, nil
	} else if elem := typeutil.ResolveValue(in); typeutil.IsStruct(elem) {
		return FromMap(typeutil.V(elem).MapNative(util.RecordStructTag))
	} else if typeutil.IsMap(in) {
		return FromMap(maputil.M(in).MapNative())
	} else if fStr, ok := in.(string); ok {
		return ParseSpec(fStr)
	} else {
		return Null(), fmt.Errorf("Expected filter.Filter, map, or string; got: %T", in)
	}
}

func MustParse(in interface{}) *Filter {
	if flt, err := Parse(in); err == nil {
		return flt
	} else {
		panic("filter: " + err.Error())
	}
}

// Filter syntax definition
func ParseSpec(spec string) (*Filter, error) {
	var criterion Criterion

	spec = strings.TrimPrefix(spec, `/`)

	if spec == AllValue {
		return All(), nil
	}

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
					var value interface{}

					value = v

					// do some extra processing for time types to make them
					// more flexible as filter criteria
					if criterion.Type == dal.TimeType {
						factor := 1

						if strings.HasPrefix(v, `-`) {
							v = v[1:]
							factor = -1
						}

						if delta, err := timeutil.ParseDuration(v); err == nil {
							value = time.Now().Add(time.Duration(factor) * delta)
						} else if tm, err := stringutil.ConvertToTime(v); err == nil {
							value = tm
						}
					}

					criterion.Values = append(criterion.Values, value)
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

func (self *Filter) AddCriteria(criteria ...Criterion) *Filter {
	self.MatchAll = false
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
	if len(self.Criteria) == 0 {
		if self.MatchAll || self.Spec == AllValue {
			self.MatchAll = true
			return true
		}
	}

	return false
}

func (self *Filter) String() string {
	if self.IsMatchAll() {
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
	if self.Conjunction == OrConjunction {
		panic("OR conjunction is not yet supported in filter.MatchesRecord()")
	}

	if self.IsMatchAll() {
		return true
	}

	if record == nil {
		return false
	}

	for _, criterion := range self.Criteria {
		var anyMatched bool

	ValuesLoop:
		for _, vI := range criterion.Values {
			vStr := typeutil.String(vI)

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

			if criterion.Field == self.IdentityField || criterion.Field == `id` {
				cmpValue = record.ID
			} else {
				cmpValue = record.Get(criterion.Field)
			}

			if cmpValue != nil {
				cmpValueS = typeutil.String(cmpValue)

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
					isEqual = (typeutil.Float(vI) == typeutil.Float(cmpValue))

				case dal.IntType:
					isEqual = (typeutil.Int(vI) == typeutil.Int(cmpValue))

				case dal.BooleanType:
					isEqual = (typeutil.Bool(vI) == typeutil.Bool(cmpValue))

				default:
					isEqual = (vI == cmpValue)
				}

				if !invertQuery && isEqual || invertQuery && !isEqual {
					anyMatched = true
					break ValuesLoop
				}

			case `prefix`:
				if strings.HasPrefix(strings.ToLower(cmpValueS), strings.ToLower(vStr)) {
					anyMatched = true
					break ValuesLoop
				}

			case `suffix`:
				if strings.HasSuffix(strings.ToLower(cmpValueS), strings.ToLower(vStr)) {
					anyMatched = true
					break ValuesLoop
				}

			case `contains`:
				if strings.Contains(strings.ToLower(cmpValueS), strings.ToLower(vStr)) {
					anyMatched = true
					break ValuesLoop
				}

			case `gt`, `lt`, `gte`, `lte`:
				cmpValueF := typeutil.Float(cmpValue)
				vF := typeutil.Float(vI)

				switch criterion.Operator {
				case `gt`:
					if cmpValueF > vF {
						anyMatched = true
						break ValuesLoop
					}
				case `gte`:
					if cmpValueF >= vF {
						anyMatched = true
						break ValuesLoop
					}
				case `lt`:
					if cmpValueF < vF {
						anyMatched = true
						break ValuesLoop
					}
				case `lte`:
					if cmpValueF <= vF {
						anyMatched = true
						break ValuesLoop
					}
				}
			default:
				return false
			}
		}

		// if none of the values matched, the criterion is false
		if !anyMatched {
			return false
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
