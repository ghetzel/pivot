package generators

import (
	"github.com/ghetzel/pivot/filter"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestSqlSelects(t *testing.T) {
	assert := require.New(t)

	fieldsets := []string{
		`*`,
		`id`,
		`id,name`,
	}

	for _, field := range fieldsets {
		tests := map[string]string{
			`all`:              `SELECT ` + field + ` FROM foo`,
			`id/1`:             `SELECT ` + field + ` FROM foo WHERE (id = 1)`,
			`id/not:1`:         `SELECT ` + field + ` FROM foo WHERE (id <> 1)`,
			`name/Bob Johnson`: `SELECT ` + field + ` FROM foo WHERE (name = 'Bob Johnson')`,
			`age/21`:           `SELECT ` + field + ` FROM foo WHERE (age = 21)`,
			`int:age/21`:       `SELECT ` + field + ` FROM foo WHERE (CAST(age AS BIGINT) = 21)`,
			`float:age/21`:     `SELECT ` + field + ` FROM foo WHERE (CAST(age AS DECIMAL) = 21)`,
			`enabled/true`:     `SELECT ` + field + ` FROM foo WHERE (enabled = true)`,
			`enabled/false`:    `SELECT ` + field + ` FROM foo WHERE (enabled = false)`,
			`enabled/null`:     `SELECT ` + field + ` FROM foo WHERE (enabled IS NULL)`,
			`enabled/not:null`: `SELECT ` + field + ` FROM foo WHERE (enabled IS NOT NULL)`,
			`age/lt:21`:        `SELECT ` + field + ` FROM foo WHERE (age < 21)`,
			`age/lte:21`:       `SELECT ` + field + ` FROM foo WHERE (age <= 21)`,
			`age/gt:21`:        `SELECT ` + field + ` FROM foo WHERE (age > 21)`,
			`age/gte:21`:       `SELECT ` + field + ` FROM foo WHERE (age >= 21)`,
			`name/contains:ob`: `SELECT ` + field + ` FROM foo WHERE (name LIKE '%%ob%%')`,
			`name/prefix:ob`:   `SELECT ` + field + ` FROM foo WHERE (name LIKE 'ob%%')`,
			`name/suffix:ob`:   `SELECT ` + field + ` FROM foo WHERE (name LIKE '%%ob')`,
			`age/7/name/ted`:   `SELECT ` + field + ` FROM foo WHERE (age = 7) AND (name = 'ted')`,
		}

		for spec, expected := range tests {
			f, err := filter.Parse(spec)
			assert.Nil(err)
			if field != `*` {
				f.Fields = strings.Split(field, `,`)
			}

			gen := NewSqlGenerator()
			actual, err := filter.Render(gen, `foo`, f)
			assert.Nil(err)
			assert.Equal(expected, string(actual[:]))
		}
	}
}

func TestSqlInserts(t *testing.T) {
	assert := require.New(t)

	tests := map[string]map[string]interface{}{
		`INSERT INTO foo (id) VALUES (1)`: map[string]interface{}{
			`id`: 1,
		},
		`INSERT INTO foo (name) VALUES ('Bob Johnson')`: map[string]interface{}{
			`name`: `Bob Johnson`,
		},
		`INSERT INTO foo (age) VALUES (21)`: map[string]interface{}{
			`age`: 21,
		},
		`INSERT INTO foo (enabled) VALUES (true)`: map[string]interface{}{
			`enabled`: true,
		},
		`INSERT INTO foo (enabled) VALUES (false)`: map[string]interface{}{
			`enabled`: false,
		},
		`INSERT INTO foo (enabled) VALUES (NULL)`: map[string]interface{}{
			`enabled`: nil,
		},
		`INSERT INTO foo (age, name) VALUES (7, 'ted')`: map[string]interface{}{
			`name`: `ted`,
			`age`:  7,
		},
	}

	for expected, input := range tests {
		f := filter.MakeFilter(``)

		gen := NewSqlGenerator()
		gen.Type = SqlInsertStatement
		gen.InputData = input

		actual, err := filter.Render(gen, `foo`, f)
		assert.Nil(err)
		assert.Equal(expected, string(actual[:]))
	}
}

type updateTestData struct {
	Input  map[string]interface{}
	Filter string
}

func TestSqlUpdates(t *testing.T) {
	assert := require.New(t)

	tests := map[string]updateTestData{
		`UPDATE foo SET (id = 1)`: updateTestData{
			Input: map[string]interface{}{
				`id`: 1,
			},
		},
		`UPDATE foo SET (name = 'Bob Johnson') WHERE (id = 1)`: updateTestData{
			Input: map[string]interface{}{
				`name`: `Bob Johnson`,
			},
			Filter: `id/1`,
		},
		`UPDATE foo SET (age = 21) WHERE (age < 21)`: updateTestData{
			Input: map[string]interface{}{
				`age`: 21,
			},
			Filter: `age/lt:21`,
		},
		`UPDATE foo SET (enabled = true) WHERE (enabled IS NULL)`: updateTestData{
			Input: map[string]interface{}{
				`enabled`: true,
			},
			Filter: `enabled/null`,
		},
		`UPDATE foo SET (enabled = false)`: updateTestData{
			Input: map[string]interface{}{
				`enabled`: false,
			},
		},
		`UPDATE foo SET (enabled = NULL)`: updateTestData{
			Input: map[string]interface{}{
				`enabled`: nil,
			},
		},
		`UPDATE foo SET (age = 7, name = 'ted') WHERE (id = 42)`: updateTestData{
			Input: map[string]interface{}{
				`name`: `ted`,
				`age`:  7,
			},
			Filter: `id/42`,
		},
	}

	for expected, testData := range tests {
		f, err := filter.Parse(testData.Filter)
		assert.Nil(err)

		gen := NewSqlGenerator()
		gen.Type = SqlUpdateStatement
		gen.InputData = testData.Input

		actual, err := filter.Render(gen, `foo`, f)
		assert.Nil(err)
		assert.Equal(expected, string(actual[:]))
	}
}
