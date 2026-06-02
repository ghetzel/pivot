package generators

import (
	"fmt"
	"regexp"

	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/v3/filter"
)

var rxCharFilter = regexp.MustCompile(`[\W\s]`)

func mongoCriterionOperatorIs(gen *MongoDB, criterion filter.Criterion) (map[string]any, error) {
	c := make(map[string]any)

	if len(criterion.Values) == 1 && criterion.Values[0] == nil {
		gen.values = append(gen.values, nil)

		c[`$or`] = []map[string]any{
			{
				criterion.Field: map[string]any{
					`$exists`: false,
				},
			}, {
				criterion.Field: nil,
			},
		}
	} else {
		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)
		}

		if len(criterion.Values) == 1 {
			if criterion.Field == `_id` {
				c[criterion.Field] = fmt.Sprintf("%v", criterion.Values[0])
			} else {
				c[criterion.Field] = criterion.Values[0]
			}
		} else {
			c[criterion.Field] = map[string]any{
				`$in`: criterion.Values,
			}
		}
	}

	return c, nil
}

func mongoCriterionOperatorNot(gen *MongoDB, criterion filter.Criterion) (map[string]any, error) {
	c := make(map[string]any)

	if len(criterion.Values) == 0 {
		return c, fmt.Errorf("The not criterion must have at least one value")

	} else if len(criterion.Values) == 1 && criterion.Values[0] == nil {
		gen.values = append(gen.values, nil)

		c[`$and`] = []map[string]any{
			{
				criterion.Field: map[string]any{
					`$exists`: true,
				},
			}, {
				criterion.Field: map[string]any{
					`$not`: nil,
				},
			},
		}
	} else {
		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)
		}

		if len(criterion.Values) == 1 {
			c[criterion.Field] = map[string]any{
				`$ne`: criterion.Values[0],
			}
		} else {
			c[criterion.Field] = map[string]any{
				`$nin`: criterion.Values,
			}
		}
	}

	return c, nil
}

func mongoCriterionOperatorPattern(gen *MongoDB, opname string, criterion filter.Criterion) (map[string]any, error) {
	c := make(map[string]any)

	if len(criterion.Values) == 0 {
		return nil, fmt.Errorf("The not criterion must have at least one value")
	} else {
		or_regexp := make([]map[string]any, 0)

		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)
			var valueClause string

			switch opname {
			case `contains`:
				valueClause = fmt.Sprintf(".*%v.*", value)
			case `prefix`:
				valueClause = fmt.Sprintf("^%v.*", value)
			case `suffix`:
				valueClause = fmt.Sprintf(".*%v$", value)
			case `like`, `unlike`:
				valueClause = rxCharFilter.ReplaceAllString(fmt.Sprintf("%v", value), `.`)
			default:
				return nil, fmt.Errorf("Unsupported pattern operator %q", opname)
			}

			if opname == `unlike` {
				or_regexp = append(or_regexp, map[string]any{
					`$not`: map[string]any{
						criterion.Field: map[string]any{
							`$regex`:   valueClause,
							`$options`: `si`,
						},
					},
				})
			} else {
				or_regexp = append(or_regexp, map[string]any{
					criterion.Field: map[string]any{
						`$regex`:   valueClause,
						`$options`: `si`,
					},
				})
			}
		}

		if len(or_regexp) == 1 {
			c = or_regexp[0]
		} else {
			c[`$or`] = or_regexp
		}
	}

	return c, nil
}

func mongoCriterionOperatorRange(gen *MongoDB, criterion filter.Criterion, operator string) (map[string]any, error) {
	c := make(map[string]any)

	switch operator {
	case `range`:
		if l := len(criterion.Values); l > 0 && (l%2 == 0) {
			or_clauses := make([]map[string]any, 0)

			for i := 0; i < l; i += 2 {
				value1 := stringutil.Autotype(criterion.Values[i])
				value2 := stringutil.Autotype(criterion.Values[i+1])

				gen.values = append(gen.values, value1, value2)

				c[criterion.Field] = map[string]any{
					`$gte`: value1,
					`$lt`:  value2,
				}

				or_clauses = append(or_clauses, c)
				c = nil
			}

			if len(or_clauses) == 1 {
				return or_clauses[0], nil
			} else {
				return map[string]any{
					`$or`: or_clauses,
				}, nil
			}
		} else {
			return c, fmt.Errorf("Ranging criteria can only accept pairs of values, %d given", l)
		}

	default:
		switch l := len(criterion.Values); l {
		case 0:
			return c, fmt.Errorf("No values given for criterion %v", criterion.Field)
		case 1:
			value := stringutil.Autotype(criterion.Values[0])
			gen.values = append(gen.values, value)

			c[criterion.Field] = map[string]any{
				`$` + operator: value,
			}
		default:
			return c, fmt.Errorf("Numeric comparators can only accept one value, %d given", l)
		}
	}

	return c, nil
}
