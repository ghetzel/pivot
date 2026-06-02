package generators

import (
	"fmt"

	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/filter"
)

var ElasticsearchExactMatchQueryType = `term`
var ElasticsearchFuzzyMatchQueryType = `match`
var ElasticsearchFulltextDefaultConjunctionAnd = true

func esCriterionOperatorIs(gen *Elasticsearch, criterion filter.Criterion) (map[string]any, error) {
	var c = make(map[string]any)

	if len(criterion.Values) == 1 && criterion.Values[0] == `null` {
		c[`missing`] = map[string]any{
			`field`:      criterion.Field,
			`existence`:  true,
			`null_value`: true,
		}

		gen.values = append(gen.values, nil)
	} else {
		var or_terms = make([]map[string]any, 0)

		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)

			or_terms = append(or_terms, map[string]any{
				ElasticsearchExactMatchQueryType: map[string]any{
					criterion.Field: value,
				},
			})

			if v, ok := gen.options[`multifield`]; ok {
				if vS, ok := v.(string); ok {
					or_terms = append(or_terms, map[string]any{
						ElasticsearchExactMatchQueryType: map[string]any{
							(criterion.Field + `.` + vS): value,
						},
					})
				}
			}
		}

		switch len(or_terms) {
		case 0:
			break
		case 1:
			for k, v := range or_terms[0] {
				c[k] = v
			}
		default:
			c[`bool`] = map[string]any{
				`should`: or_terms,
			}
		}
	}

	return c, nil
}

func esCriterionOperatorNot(gen *Elasticsearch, criterion filter.Criterion) (map[string]any, error) {
	var c = make(map[string]any)

	if len(criterion.Values) == 0 {
		return c, fmt.Errorf("The not criterion must have at least one value")

	} else if len(criterion.Values) == 1 && criterion.Values[0] == `null` {
		gen.values = append(gen.values, nil)

		c[`bool`] = map[string]any{
			`must_not`: map[string]any{
				`missing`: map[string]any{
					`field`:      criterion.Field,
					`existence`:  true,
					`null_value`: true,
				},
			},
		}
	} else {
		and_not := make([]map[string]any, 0)

		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)

			//  strings get treated as regular expressions
			if criterion.Type == `str` {
				and_not = append(and_not, map[string]any{
					`bool`: map[string]any{
						`must_not`: map[string]any{
							`regexp`: map[string]any{
								criterion.Field: map[string]any{
									`value`: value,
									`flags`: `ALL`,
								},
							},
						},
					},
				})
			} else {
				//  all other types are a simple term match
				and_not = append(and_not, map[string]any{
					`bool`: map[string]any{
						`must_not`: map[string]any{
							ElasticsearchExactMatchQueryType: map[string]any{
								criterion.Field: value,
							},
						},
					},
				})
			}
		}

		c[`bool`] = map[string]any{
			`must`: and_not,
		}
	}

	return c, nil
}

func esCriterionOperatorLike(gen *Elasticsearch, criterion filter.Criterion) (map[string]any, error) {
	var c = make(map[string]any)

	if len(criterion.Values) == 1 && criterion.Values[0] == `null` {
		return esCriterionOperatorIs(gen, criterion)
	} else {
		var or_terms = make([]map[string]any, 0)

		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)

			or_terms = append(or_terms, map[string]any{
				ElasticsearchFuzzyMatchQueryType: map[string]any{
					criterion.Field: map[string]any{
						`query`: value,
					},
				},
			})
		}

		switch len(or_terms) {
		case 0:
			break
		case 1:
			for k, v := range or_terms[0] {
				c[k] = v
			}
		default:
			c[`bool`] = map[string]any{
				`should`: or_terms,
			}
		}
	}

	return c, nil
}

func esCriterionOperatorUnlike(gen *Elasticsearch, criterion filter.Criterion) (map[string]any, error) {
	if like, err := esCriterionOperatorLike(gen, criterion); err == nil {
		return map[string]any{
			`bool`: map[string]any{
				`must_not`: like,
			},
		}, nil
	} else {
		return nil, err
	}
}

func esCriterionOperatorPattern(gen *Elasticsearch, opname string, criterion filter.Criterion) (map[string]any, error) {
	var c = make(map[string]any)

	if len(criterion.Values) == 0 {
		return nil, fmt.Errorf("The not criterion must have at least one value")
	} else {
		var or_regexp = make([]map[string]any, 0)

		for _, value := range criterion.Values {
			gen.values = append(gen.values, value)
			var valueClause string

			switch opname {
			case `contains`:
				valueClause = fmt.Sprintf(".*%s.*", value)
			case `prefix`:
				valueClause = fmt.Sprintf("%s.*", value)
			case `suffix`:
				valueClause = fmt.Sprintf(".*%s", value)
			default:
				return nil, fmt.Errorf("Unsupported pattern operator %q", opname)
			}

			or_regexp = append(or_regexp, map[string]any{
				`regexp`: map[string]any{
					criterion.Field: map[string]any{
						`value`: valueClause,
						`flags`: `ALL`,
					},
				},
			})

			if v, ok := gen.options[`multifield`]; ok {
				if vS, ok := v.(string); ok {
					or_regexp = append(or_regexp, map[string]any{
						`regexp`: map[string]any{
							(criterion.Field + `.` + vS): map[string]any{
								`value`: valueClause,
								`flags`: `ALL`,
							},
						},
					})
				}
			}
		}

		switch len(or_regexp) {
		case 0:
			break
		case 1:
			for k, v := range or_regexp[0] {
				c[k] = v
			}
		default:
			c[`bool`] = map[string]any{
				`should`: or_regexp,
			}
		}
	}

	return c, nil
}

func esCriterionOperatorRange(gen *Elasticsearch, criterion filter.Criterion, operator string) (map[string]any, error) {
	var c = make(map[string]any)

	if l := len(criterion.Values); l == 1 {
		gen.values = append(gen.values, criterion.Values[0])

		c[`range`] = map[string]any{
			criterion.Field: map[string]any{
				operator: criterion.Values[0],
			},
		}
	} else {
		return c, fmt.Errorf("Ranging criteria can only accept one value, %d given", l)
	}

	return c, nil
}

func esCriterionOperatorFulltext(gen *Elasticsearch, criterion filter.Criterion) (map[string]any, error) {
	var c = make(map[string]any)

	var or_queries = make([]map[string]any, 0)
	var defop string

	if ElasticsearchFulltextDefaultConjunctionAnd {
		defop = `AND`
	} else {
		defop = `OR`
	}

	for _, value := range criterion.Values {
		gen.values = append(gen.values, value)

		or_queries = append(or_queries, map[string]any{
			`query_string`: map[string]any{
				`query`:            typeutil.String(value),
				`default_field`:    criterion.Field,
				`default_operator`: defop,
				`lenient`:          true,
			},
		})
	}

	switch len(or_queries) {
	case 0:
		break
	case 1:
		for k, v := range or_queries[0] {
			c[k] = v
		}
	default:
		c[`bool`] = map[string]any{
			`should`: or_queries,
		}
	}

	return c, nil
}
