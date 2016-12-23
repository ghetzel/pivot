package filter

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

var CriteriaSeparator = `/`
var ModifierDelimiter = `:`
var ValueSeparator = `|`
var QueryUnescapeValues = false

type Criterion struct {
	Type     string   `json:"type,omitempty"`
	Length   int      `json:"length,omitempty"`
	Field    string   `json:"field"`
	Operator string   `json:"operator,omitempty"`
	Values   []string `json:"values"`
}

type Filter struct {
	Spec     string
	MatchAll bool
	Offset   int
	Size     int
	Limit    int
	Criteria []Criterion
	Sort     []string
	Fields   []string
	Options  map[string]string
}

func MakeFilter(spec string) Filter {
	return Filter{
		Spec:     spec,
		Criteria: make([]Criterion, 0),
		Sort:     make([]string, 0),
		Fields:   make([]string, 0),
		Options:  make(map[string]string),
	}
}

var NullFilter Filter = MakeFilter(``)

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

	spec = strings.TrimPrefix(spec, CriteriaSeparator)

	rv := MakeFilter(spec)
	criteria := strings.Split(spec, CriteriaSeparator)

	switch {
	case spec == ``:
		return NullFilter, nil

	case spec == `all`:
		rv.MatchAll = true
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
