package generators

import (
	"testing"

	"encoding/json"

	"github.com/ghetzel/pivot/v3/filter"
	"github.com/stretchr/testify/require"
)

type mongoQv struct {
	query  map[string]any
	values []any
	input  map[string]any
}

func TestMongodb(t *testing.T) {
	assert := require.New(t)

	tests := map[string]mongoQv{
		`all`: {
			query:  map[string]any{},
			values: []any{},
		},
		`id/1`: {
			query: map[string]any{
				`_id`: `1`,
			},
			values: []any{int64(1)},
		},
		`id/not:1`: {
			query: map[string]any{
				`_id`: map[string]any{
					`$ne`: float64(1),
				},
			},
			values: []any{int64(1)},
		},
		`name/Bob Johnson`: {
			query: map[string]any{
				`name`: `Bob Johnson`,
			},
			values: []any{`Bob Johnson`},
		},
		`age/21`: {
			query: map[string]any{
				`age`: float64(21),
			},
			values: []any{int64(21)},
		},
		`enabled/true`: {
			query: map[string]any{
				`enabled`: true,
			},
			values: []any{true},
		},
		`enabled/false`: {
			query: map[string]any{
				`enabled`: false,
			},
			values: []any{false},
		},
		`enabled/null`: {
			query: map[string]any{
				`$or`: []any{
					map[string]any{
						`enabled`: map[string]any{
							`$exists`: false,
						},
					},
					map[string]any{
						`enabled`: nil,
					},
				},
			},
			values: []any{nil},
		},
		`enabled/not:null`: {
			query: map[string]any{
				`$and`: []any{
					map[string]any{
						`enabled`: map[string]any{
							`$exists`: true,
						},
					},
					map[string]any{
						`enabled`: map[string]any{
							`$not`: nil,
						},
					},
				},
			},
			values: []any{nil},
		},
		`age/lt:21`: {
			query: map[string]any{
				`age`: map[string]any{
					`$lt`: float64(21),
				},
			},
			values: []any{int64(21)},
		},
		`age/lte:21`: {
			query: map[string]any{
				`age`: map[string]any{
					`$lte`: float64(21),
				},
			},
			values: []any{int64(21)},
		},
		`age/gt:21`: {
			query: map[string]any{
				`age`: map[string]any{
					`$gt`: float64(21),
				},
			},
			values: []any{int64(21)},
		},
		`age/gte:21`: {
			query: map[string]any{
				`age`: map[string]any{
					`$gte`: float64(21),
				},
			},
			values: []any{int64(21)},
		},
		`factor/lt:3.141597`: {
			query: map[string]any{
				`factor`: map[string]any{
					`$lt`: float64(3.141597),
				},
			},
			values: []any{float64(3.141597)},
		},
		`factor/lte:3.141597`: {
			query: map[string]any{
				`factor`: map[string]any{
					`$lte`: float64(3.141597),
				},
			},
			values: []any{float64(3.141597)},
		},
		`factor/gt:3.141597`: {
			query: map[string]any{
				`factor`: map[string]any{
					`$gt`: float64(3.141597),
				},
			},
			values: []any{float64(3.141597)},
		},
		`factor/gte:3.141597`: {
			query: map[string]any{
				`factor`: map[string]any{
					`$gte`: float64(3.141597),
				},
			},
			values: []any{float64(3.141597)},
		},
		`name/contains:ob`: {
			query: map[string]any{
				`name`: map[string]any{
					`$regex`:   `.*ob.*`,
					`$options`: `si`,
				},
			},
			values: []any{`ob`},
		},
		`name/prefix:ob`: {
			query: map[string]any{
				`name`: map[string]any{
					`$regex`:   `^ob.*`,
					`$options`: `si`,
				},
			},
			values: []any{`ob`},
		},
		`name/suffix:ob`: {
			query: map[string]any{
				`name`: map[string]any{
					`$regex`:   `.*ob$`,
					`$options`: `si`,
				},
			},
			values: []any{`ob`},
		},
		`age/7/name/ted`: {
			query: map[string]any{
				`$and`: []any{
					map[string]any{
						`age`: float64(7),
					},
					map[string]any{
						`name`: `ted`,
					},
				},
			},
			values: []any{int64(7), `ted`},
		},
	}

	for spec, expected := range tests {
		f, err := filter.Parse(spec)
		assert.NoError(err)

		gen := NewMongoDBGenerator()
		actual, err := filter.Render(gen, `foo`, f)
		assert.NoError(err)

		var query map[string]any
		assert.NoError(json.Unmarshal(actual, &query))

		assert.Equal(expected.query, query, "filter: %v", spec)
		assert.Equal(expected.values, gen.GetValues(), "filter: %v", spec)
	}
}
