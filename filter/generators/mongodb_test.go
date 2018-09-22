package generators

import (
	"testing"

	"encoding/json"

	"github.com/ghetzel/pivot/v3/filter"
	"github.com/stretchr/testify/require"
)

type mongoQv struct {
	query  map[string]interface{}
	values []interface{}
	input  map[string]interface{}
}

func TestMongodb(t *testing.T) {
	assert := require.New(t)

	tests := map[string]mongoQv{
		`all`: {
			query:  map[string]interface{}{},
			values: []interface{}{},
		},
		`id/1`: {
			query: map[string]interface{}{
				`_id`: `1`,
			},
			values: []interface{}{int64(1)},
		},
		`id/not:1`: {
			query: map[string]interface{}{
				`_id`: map[string]interface{}{
					`$ne`: float64(1),
				},
			},
			values: []interface{}{int64(1)},
		},
		`name/Bob Johnson`: {
			query: map[string]interface{}{
				`name`: `Bob Johnson`,
			},
			values: []interface{}{`Bob Johnson`},
		},
		`age/21`: {
			query: map[string]interface{}{
				`age`: float64(21),
			},
			values: []interface{}{int64(21)},
		},
		`enabled/true`: {
			query: map[string]interface{}{
				`enabled`: true,
			},
			values: []interface{}{true},
		},
		`enabled/false`: {
			query: map[string]interface{}{
				`enabled`: false,
			},
			values: []interface{}{false},
		},
		`enabled/null`: {
			query: map[string]interface{}{
				`$or`: []interface{}{
					map[string]interface{}{
						`enabled`: map[string]interface{}{
							`$exists`: false,
						},
					},
					map[string]interface{}{
						`enabled`: nil,
					},
				},
			},
			values: []interface{}{nil},
		},
		`enabled/not:null`: {
			query: map[string]interface{}{
				`$and`: []interface{}{
					map[string]interface{}{
						`enabled`: map[string]interface{}{
							`$exists`: true,
						},
					},
					map[string]interface{}{
						`enabled`: map[string]interface{}{
							`$not`: nil,
						},
					},
				},
			},
			values: []interface{}{nil},
		},
		`age/lt:21`: {
			query: map[string]interface{}{
				`age`: map[string]interface{}{
					`$lt`: float64(21),
				},
			},
			values: []interface{}{int64(21)},
		},
		`age/lte:21`: {
			query: map[string]interface{}{
				`age`: map[string]interface{}{
					`$lte`: float64(21),
				},
			},
			values: []interface{}{int64(21)},
		},
		`age/gt:21`: {
			query: map[string]interface{}{
				`age`: map[string]interface{}{
					`$gt`: float64(21),
				},
			},
			values: []interface{}{int64(21)},
		},
		`age/gte:21`: {
			query: map[string]interface{}{
				`age`: map[string]interface{}{
					`$gte`: float64(21),
				},
			},
			values: []interface{}{int64(21)},
		},
		`factor/lt:3.141597`: {
			query: map[string]interface{}{
				`factor`: map[string]interface{}{
					`$lt`: float64(3.141597),
				},
			},
			values: []interface{}{float64(3.141597)},
		},
		`factor/lte:3.141597`: {
			query: map[string]interface{}{
				`factor`: map[string]interface{}{
					`$lte`: float64(3.141597),
				},
			},
			values: []interface{}{float64(3.141597)},
		},
		`factor/gt:3.141597`: {
			query: map[string]interface{}{
				`factor`: map[string]interface{}{
					`$gt`: float64(3.141597),
				},
			},
			values: []interface{}{float64(3.141597)},
		},
		`factor/gte:3.141597`: {
			query: map[string]interface{}{
				`factor`: map[string]interface{}{
					`$gte`: float64(3.141597),
				},
			},
			values: []interface{}{float64(3.141597)},
		},
		`name/contains:ob`: {
			query: map[string]interface{}{
				`name`: map[string]interface{}{
					`$regex`:   `.*ob.*`,
					`$options`: `si`,
				},
			},
			values: []interface{}{`ob`},
		},
		`name/prefix:ob`: {
			query: map[string]interface{}{
				`name`: map[string]interface{}{
					`$regex`:   `^ob.*`,
					`$options`: `si`,
				},
			},
			values: []interface{}{`ob`},
		},
		`name/suffix:ob`: {
			query: map[string]interface{}{
				`name`: map[string]interface{}{
					`$regex`:   `.*ob$`,
					`$options`: `si`,
				},
			},
			values: []interface{}{`ob`},
		},
		`age/7/name/ted`: {
			query: map[string]interface{}{
				`$and`: []interface{}{
					map[string]interface{}{
						`age`: float64(7),
					},
					map[string]interface{}{
						`name`: `ted`,
					},
				},
			},
			values: []interface{}{int64(7), `ted`},
		},
	}

	for spec, expected := range tests {
		f, err := filter.Parse(spec)
		assert.Nil(err)

		gen := NewMongoDBGenerator()
		actual, err := filter.Render(gen, `foo`, f)
		assert.NoError(err)

		var query map[string]interface{}
		assert.NoError(json.Unmarshal(actual, &query))

		assert.Equal(expected.query, query, "filter: %v", spec)
		assert.Equal(expected.values, gen.GetValues(), "filter: %v", spec)
	}
}
