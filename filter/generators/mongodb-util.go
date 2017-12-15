package generators

import (
	"fmt"

	"github.com/ghetzel/pivot/filter"
)

func mongoCriterionOperatorIs(gen *MongoDB, criterion filter.Criterion) (map[string]interface{}, error) {
	c := make(map[string]interface{})

	if len(criterion.Values) == 1 && criterion.Values[0] == `null` {
		gen.values = append(gen.values, nil)

		c[`$or`] = []map[string]interface{}{
			{
				criterion.Field: map[string]interface{}{
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
			c[criterion.Field] = criterion.Values[0]
		} else {
			c[criterion.Field] = map[string]interface{}{
				`$in`: criterion.Values,
			}
		}
	}

	return c, nil
}

func mongoCriterionOperatorNot(gen *MongoDB, criterion filter.Criterion) (map[string]interface{}, error) {
	c := make(map[string]interface{})

	if len(criterion.Values) == 0 {
		return c, fmt.Errorf("The not criterion must have at least one value")

	} else if len(criterion.Values) == 1 && criterion.Values[0] == `null` {
		gen.values = append(gen.values, nil)

		c[`$and`] = []map[string]interface{}{
			{
				criterion.Field: map[string]interface{}{
					`$exists`: true,
				},
			}, {
				criterion.Field: map[string]interface{}{
					`$not`: nil,
				},
			},
		}
	} else {
		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)
		}

		if len(criterion.Values) == 1 {
			c[criterion.Field] = map[string]interface{}{
				`$ne`: criterion.Values[0],
			}
		} else {
			c[criterion.Field] = map[string]interface{}{
				`$nin`: criterion.Values,
			}
		}
	}

	return c, nil
}

func mongoCriterionOperatorPattern(gen *MongoDB, opname string, criterion filter.Criterion) (map[string]interface{}, error) {
	c := make(map[string]interface{})

	if len(criterion.Values) == 0 {
		return nil, fmt.Errorf("The not criterion must have at least one value")
	} else {
		or_regexp := make([]map[string]interface{}, 0)

		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)
			var valueClause string

			switch opname {
			case `contains`:
				valueClause = fmt.Sprintf(".*%s.*", value)
			case `prefix`:
				valueClause = fmt.Sprintf("^%s.*", value)
			case `suffix`:
				valueClause = fmt.Sprintf(".*%s$", value)
			default:
				return nil, fmt.Errorf("Unsupported pattern operator %q", opname)
			}

			or_regexp = append(or_regexp, map[string]interface{}{
				criterion.Field: map[string]interface{}{
					`$regexp`:  valueClause,
					`$options`: `si`,
				},
			})
		}

		if len(or_regexp) == 1 {
			c = or_regexp[0]
		} else {
			c[`$or`] = or_regexp
		}
	}

	return c, nil
}

func mongoCriterionOperatorRange(gen *MongoDB, criterion filter.Criterion, operator string) (map[string]interface{}, error) {
	c := make(map[string]interface{})

	switch operator {
	case `range`:
		if l := len(criterion.Values); l > 0 && (l%2 == 0) {
			gen.values = append(gen.values, criterion.Values[0])
			or_clauses := make([]map[string]interface{}, 0)

			for i := 0; i < l; i += 2 {
				c[criterion.Field] = map[string]interface{}{
					`$gte`: criterion.Values[i],
					`$lt`:  criterion.Values[i+1],
				}

				or_clauses = append(or_clauses, c)
				c = nil
			}

			if len(or_clauses) == 1 {
				return or_clauses[0], nil
			} else {
				return map[string]interface{}{
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
			gen.values = append(gen.values, criterion.Values[0])

			c[criterion.Field] = map[string]interface{}{
				`$` + operator: criterion.Values[0],
			}
		default:
			return c, fmt.Errorf("Numeric comparators can only accept one value, %d given", l)
		}
	}

	return c, nil
}
