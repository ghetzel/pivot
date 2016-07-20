package generators

import (
	"fmt"
	"github.com/ghetzel/pivot/filter"
)

func esCriterionOperatorIs(gen *Elasticsearch, criterion filter.Criterion) (map[string]interface{}, error) {
	c := make(map[string]interface{})

	if len(criterion.Values) == 1 && criterion.Values[0] == `null` {
		c[`missing`] = map[string]interface{}{
			`field`:      criterion.Field,
			`existence`:  true,
			`null_value`: true,
		}
	} else {
		or_terms := make([]map[string]interface{}, 0)

		for _, value := range criterion.Values {
			or_terms = append(or_terms, map[string]interface{}{
				`term`: map[string]interface{}{
					criterion.Field: value,
				},
			})

			if v, ok := gen.options[`multifield`]; ok {
				or_terms = append(or_terms, map[string]interface{}{
					`term`: map[string]interface{}{
						(criterion.Field + `.` + v): value,
					},
				})
			}

			c[`or`] = or_terms
		}
	}

	return c, nil
}

func esCriterionOperatorNot(gen *Elasticsearch, criterion filter.Criterion) (map[string]interface{}, error) {
	c := make(map[string]interface{})

	if len(criterion.Values) == 0 {
		return c, fmt.Errorf("The not criterion must have at least one value")

	} else if len(criterion.Values) == 1 && criterion.Values[0] == `null` {
		c[`bool`] = map[string]interface{}{
			`must_not`: map[string]interface{}{
				`missing`: map[string]interface{}{
					`field`:      criterion.Field,
					`existence`:  true,
					`null_value`: true,
				},
			},
		}
	} else {
		and_not := make([]map[string]interface{}, 0)

		for _, value := range criterion.Values {
			//  strings get treated as regular expressions
			if criterion.Type == `str` {
				and_not = append(and_not, map[string]interface{}{
					`bool`: map[string]interface{}{
						`must_not`: map[string]interface{}{
							`regexp`: map[string]interface{}{
								criterion.Field: map[string]interface{}{
									`value`: value,
									`flags`: `ALL`,
								},
							},
						},
					},
				})
			} else {
				//  all other types are a simple term match
				and_not = append(and_not, map[string]interface{}{
					`bool`: map[string]interface{}{
						`must_not`: map[string]interface{}{
							`term`: map[string]interface{}{
								criterion.Field: value,
							},
						},
					},
				})
			}
		}

		c[`and`] = and_not
	}

	return c, nil
}

func esCriterionOperatorContains(gen *Elasticsearch, criterion filter.Criterion) (map[string]interface{}, error) {
	c := make(map[string]interface{})

	if len(criterion.Values) == 0 {
		return c, fmt.Errorf("The not criterion must have at least one value")
	} else {
		or_regexp := make([]map[string]interface{}, 0)

		for _, value := range criterion.Values {
			or_regexp = append(or_regexp, map[string]interface{}{
				`regexp`: map[string]interface{}{
					criterion.Field: map[string]interface{}{
						`value`: fmt.Sprintf(".*%s.*", value),
						`flags`: `ALL`,
					},
				},
			})

			if v, ok := gen.options[`multifield`]; ok {
				or_regexp = append(or_regexp, map[string]interface{}{
					`regexp`: map[string]interface{}{
						(criterion.Field + `.` + v): map[string]interface{}{
							`value`: fmt.Sprintf(".*%s.*", value),
							`flags`: `ALL`,
						},
					},
				})
			}
		}

		c[`or`] = or_regexp
	}

	return c, nil
}

func esCriterionOperatorRange(gen *Elasticsearch, criterion filter.Criterion, operator string) (map[string]interface{}, error) {
	c := make(map[string]interface{})

	if l := len(criterion.Values); l == 1 {
		c[`range`] = map[string]interface{}{
			criterion.Field: map[string]interface{}{
				operator: criterion.Values[0],
			},
		}
	} else {
		return c, fmt.Errorf("Ranging criteria can only accept one value, %d given", l)
	}

	return c, nil
}
