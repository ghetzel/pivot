package filter

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

var CriteriaSeparator = `/`
var FieldTermSeparator = `/`
var ModifierDelimiter = `:`
var ValueSeparator = `|`
var QueryUnescapeValues = false
var IdentityField = `_id`
var AllValue = `all`

type Criterion struct {
	Type     string   `json:"type,omitempty"`
	Length   int      `json:"length,omitempty"`
	Field    string   `json:"field"`
	Operator string   `json:"operator,omitempty"`
	Values   []string `json:"values"`
}

func (self *Criterion) String() string {
	rv := ``

	if self.Type != `` {
		if self.Length > 0 {
			rv += self.Type + fmt.Sprintf("#%d", self.Length) + ModifierDelimiter
		} else {
			rv += self.Type + ModifierDelimiter
		}
	}

	rv += self.Field + FieldTermSeparator

	if self.Operator != `` {
		rv += self.Operator + ModifierDelimiter
	}

	values := make([]string, 0)

	for _, value := range self.Values {
		if QueryUnescapeValues {
			values = append(values, url.QueryEscape(value))
		} else {
			values = append(values, value)
		}
	}

	rv += strings.Join(values, ValueSeparator)

	return rv
}

type Filter struct {
	Spec     string
	MatchAll bool
	Offset   int
	Limit    int
	Criteria []Criterion
	Sort     []string
	Fields   []string
	Options  map[string]string
}

func MakeFilter(spec string) Filter {
	f := Filter{
		Spec:     spec,
		Criteria: make([]Criterion, 0),
		Sort:     make([]string, 0),
		Fields:   make([]string, 0),
		Options:  make(map[string]string),
	}

	if spec == AllValue {
		f.MatchAll = true
	}

	return f
}

func FromMap(in map[string]interface{}) (Filter, error) {
	criteria := make([]string, 0)

	for typeField, opValue := range in {
		criteria = append(criteria, fmt.Sprintf("%s%s%v", typeField, FieldTermSeparator, opValue))
	}

	return Parse(strings.Join(criteria, CriteriaSeparator))
}

var Null Filter = MakeFilter(``)
var All Filter = MakeFilter(AllValue)

// Filter syntax definition
//
// filter     ::= ([sort]field/value | [sort]type:field/value | [sort]type:field/comparator:value)+
// sort       ::= ASCII plus (+), minus (-)
// field      ::= ? US-ASCII field name ?;
// value      ::= ? UTF-8 field value ?;
// type       ::= str | bool | int | float | date
// comparator :=  is | not | gt | gte | lt | lte | prefix | suffix | regex
//
func Parse(spec string) (Filter, error) {
	var criterion Criterion

	spec = strings.TrimPrefix(spec, `/`)

	rv := MakeFilter(spec)
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
		return Null, nil

	case spec == AllValue:
		return rv, nil

	case len(criteria) >= 2:
		for i, token := range criteria {
			if (i % 2) == 0 {
				parts := strings.SplitN(token, ModifierDelimiter, 2)

				var addSortAsc *bool

				if strings.HasPrefix(token, `+`) {
					v := true
					addSortAsc = &v
				} else if strings.HasPrefix(token, `-`) {
					v := false
					addSortAsc = &v
				}

				if len(parts) == 1 {
					criterion = Criterion{
						Field: parts[0],
					}
				} else {
					typeLengthPair := strings.SplitN(parts[0], `#`, 2)

					if len(typeLengthPair) == 1 {
						criterion = Criterion{
							Type:  parts[0],
							Field: parts[1],
						}
					} else {
						if v, err := strconv.ParseUint(typeLengthPair[1], 10, 32); err == nil {
							criterion = Criterion{
								Type:   typeLengthPair[0],
								Length: int(v),
								Field:  parts[1],
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
						rv.Sort = append(rv.Sort, `-`+criterion.Field)
					}
				}
			} else {
				parts := strings.SplitN(token, ModifierDelimiter, 2)

				if len(parts) == 1 {
					criterion.Values = strings.Split(parts[0], ValueSeparator)
				} else {
					criterion.Operator = parts[0]
					criterion.Values = strings.Split(parts[1], ValueSeparator)
				}

				if QueryUnescapeValues {
					for i, value := range criterion.Values {
						if v, err := url.QueryUnescape(value); err == nil {
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
		return rv, fmt.Errorf("Invalid filter spec %q", spec)
	}

	return rv, nil
}

func (self *Filter) CriteriaFields() []string {
	fields := make([]string, len(self.Criteria))

	for i, criterion := range self.Criteria {
		fields[i] = criterion.Field
	}

	return fields
}

func (self *Filter) IdOnly() bool {
	if self.Fields != nil && len(self.Fields) == 1 && self.Fields[0] == IdentityField {
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
