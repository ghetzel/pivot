package filter

import (
	"strconv"
	"strings"
)

type Criterion struct {
	Type     string   `json:"type,omitempty"`
	Length   int      `json:"length,omitempty"`
	Field    string   `json:"field"`
	Operator string   `json:"operator,omitempty"`
	Values   []string `json:"values"`
}

type Filter struct {
	Spec     string
	Criteria []Criterion
	Fields   []string
	Options  map[string]string
}

func Parse(spec string) (Filter, error) {
	var criterion Criterion

	spec = strings.TrimPrefix(spec, `/`)

	rv := Filter{
		Spec:     spec,
		Criteria: make([]Criterion, 0),
		Fields:   make([]string, 0),
		Options:  make(map[string]string),
	}

	criteria := strings.Split(spec, `/`)

	if len(criteria) >= 2 {
		for i, token := range criteria {
			if (i % 2) == 0 {
				parts := strings.SplitN(token, `:`, 2)

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
			} else {
				parts := strings.SplitN(token, `:`, 2)

				if len(parts) == 1 {
					criterion.Values = strings.Split(parts[0], `|`)
				} else {
					criterion.Operator = parts[0]
					criterion.Values = strings.Split(parts[1], `|`)
				}

				rv.Criteria = append(rv.Criteria, criterion)
			}
		}
	}

	return rv, nil
}
