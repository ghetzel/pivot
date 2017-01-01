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
