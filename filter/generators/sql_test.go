package generators

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/stretchr/testify/require"
)

type qv struct {
	query   string
	values  []interface{}
	input   map[string]interface{}
	mapping SqlTypeMapping
}

func TestSqlSplitTypeLength(t *testing.T) {
	assert := require.New(t)

	gen := NewSqlGenerator()

	typeName, length, precision := gen.SplitTypeLength(`integer`)
	assert.Equal(`INTEGER`, typeName)
	assert.Equal(0, length)
	assert.Equal(0, precision)

	typeName, length, precision = gen.SplitTypeLength(`integer(1)`)
	assert.Equal(`INTEGER`, typeName)
	assert.Equal(1, length)
	assert.Equal(0, precision)

	typeName, length, precision = gen.SplitTypeLength(`float(255,12)`)
	assert.Equal(`FLOAT`, typeName)
	assert.Equal(255, length)
	assert.Equal(12, precision)

	typeName, length, precision = gen.SplitTypeLength(`INTEGER`)
	assert.Equal(`INTEGER`, typeName)
	assert.Equal(0, length)
	assert.Equal(0, precision)

	typeName, length, precision = gen.SplitTypeLength(`INTEGER(1)`)
	assert.Equal(`INTEGER`, typeName)
	assert.Equal(1, length)
	assert.Equal(0, precision)

	typeName, length, precision = gen.SplitTypeLength(`FLOAT(255,12)`)
	assert.Equal(`FLOAT`, typeName)
	assert.Equal(255, length)
	assert.Equal(12, precision)
}

func TestSqlSelects(t *testing.T) {
	assert := require.New(t)

	fieldsets := []string{
		`*`,
		`id`,
		`id,name`,
	}

	for _, field := range fieldsets {
		tests := map[string]qv{
			`all`: {
				query:   `SELECT ` + field + ` FROM foo`,
				values:  []interface{}{},
				mapping: DefaultSqlTypeMapping,
			},
			`id/1`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (id = ?)`,
				values:  []interface{}{int64(1)},
				mapping: DefaultSqlTypeMapping,
			},
			`id/not:1`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (id <> ?)`,
				values:  []interface{}{int64(1)},
				mapping: DefaultSqlTypeMapping,
			},
			`name/Bob Johnson`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (name = ?)`,
				values:  []interface{}{`Bob Johnson`},
				mapping: DefaultSqlTypeMapping,
			},
			`age/21`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (age = ?)`,
				values:  []interface{}{int64(21)},
				mapping: DefaultSqlTypeMapping,
			},
			`enabled/true`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (enabled = ?)`,
				values:  []interface{}{true},
				mapping: DefaultSqlTypeMapping,
			},
			`enabled/false`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (enabled = ?)`,
				values:  []interface{}{false},
				mapping: DefaultSqlTypeMapping,
			},
			`enabled/null`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (enabled IS NULL)`,
				values:  []interface{}{nil},
				mapping: DefaultSqlTypeMapping,
			},
			`enabled/not:null`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (enabled IS NOT NULL)`,
				values:  []interface{}{nil},
				mapping: DefaultSqlTypeMapping,
			},
			`age/lt:21`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (age < ?)`,
				values:  []interface{}{int64(21)},
				mapping: DefaultSqlTypeMapping,
			},
			`age/lte:21`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (age <= ?)`,
				values:  []interface{}{int64(21)},
				mapping: DefaultSqlTypeMapping,
			},
			`age/gt:21`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (age > ?)`,
				values:  []interface{}{int64(21)},
				mapping: DefaultSqlTypeMapping,
			},
			`age/gte:21`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (age >= ?)`,
				values:  []interface{}{int64(21)},
				mapping: DefaultSqlTypeMapping,
			},
			`factor/lt:3.141597`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (factor < ?)`,
				values:  []interface{}{float64(3.141597)},
				mapping: DefaultSqlTypeMapping,
			},
			`factor/lte:3.141597`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (factor <= ?)`,
				values:  []interface{}{float64(3.141597)},
				mapping: DefaultSqlTypeMapping,
			},
			`factor/gt:3.141597`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (factor > ?)`,
				values:  []interface{}{float64(3.141597)},
				mapping: DefaultSqlTypeMapping,
			},
			`factor/gte:3.141597`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (factor >= ?)`,
				values:  []interface{}{float64(3.141597)},
				mapping: DefaultSqlTypeMapping,
			},
			`name/contains:ob`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (name LIKE ?)`,
				values:  []interface{}{`%%ob%%`},
				mapping: DefaultSqlTypeMapping,
			},
			`name/prefix:ob`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (name LIKE ?)`,
				values:  []interface{}{`ob%%`},
				mapping: DefaultSqlTypeMapping,
			},
			`name/suffix:ob`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (name LIKE ?)`,
				values:  []interface{}{`%%ob`},
				mapping: DefaultSqlTypeMapping,
			},
			`age/7/name/ted`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (age = ?) AND (name = ?)`,
				values:  []interface{}{int64(7), `ted`},
				mapping: DefaultSqlTypeMapping,
			},
			`factor/range:42|55`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (factor BETWEEN ? AND ?)`,
				values:  []interface{}{int64(42), int64(55)},
				mapping: DefaultSqlTypeMapping,
			},
			`factor/range:2006-01-02T00:00:00Z|2006-01-13T00:00:00Z`: {
				query: `SELECT ` + field + ` FROM foo WHERE (factor BETWEEN ? AND ?)`,
				values: []interface{}{
					time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
					time.Date(2006, 1, 13, 0, 0, 0, 0, time.UTC),
				},
				mapping: DefaultSqlTypeMapping,
			},
			`name/bob|alice|mary`: {
				query:   `SELECT ` + field + ` FROM foo WHERE (name IN(?, ?, ?))`,
				values:  []interface{}{`bob`, `alice`, `mary`},
				mapping: DefaultSqlTypeMapping,
			},
		}

		for spec, expected := range tests {
			f, err := filter.Parse(spec)
			assert.Nil(err)
			if field != `*` {
				f.Fields = strings.Split(field, `, `)
			}

			gen := NewSqlGenerator()
			gen.TypeMapping = expected.mapping
			actual, err := filter.Render(gen, `foo`, f)
			assert.Nil(err)
			assert.Equal(expected.query, string(actual[:]))
			assert.Equal(expected.values, gen.GetValues())
		}
	}
}

func TestSqlInserts(t *testing.T) {
	assert := require.New(t)

	tests := []qv{
		{
			query:  `INSERT INTO foo (id) VALUES (?)`,
			values: nil,
			input: map[string]interface{}{
				`id`: 1,
			},
			mapping: DefaultSqlTypeMapping,
		}, {
			query:  `INSERT INTO foo (name) VALUES (?)`,
			values: nil,
			input: map[string]interface{}{
				`name`: `Bob Johnson`,
			},
			mapping: DefaultSqlTypeMapping,
		}, {
			query:  `INSERT INTO foo (age) VALUES (?)`,
			values: nil,
			input: map[string]interface{}{
				`age`: 21,
			},
			mapping: DefaultSqlTypeMapping,
		}, {
			query:  `INSERT INTO foo (enabled) VALUES (?)`,
			values: nil,
			input: map[string]interface{}{
				`enabled`: true,
			},
			mapping: DefaultSqlTypeMapping,
		}, {
			query:  `INSERT INTO foo (enabled) VALUES (?)`,
			values: nil,
			input: map[string]interface{}{
				`enabled`: false,
			},
			mapping: DefaultSqlTypeMapping,
		}, {
			query:  `INSERT INTO foo (enabled) VALUES (?)`,
			values: nil,
			input: map[string]interface{}{
				`enabled`: nil,
			},
			mapping: DefaultSqlTypeMapping,
		}, {
			query:  `INSERT INTO foo (age, name) VALUES (?, ?)`,
			values: nil,
			input: map[string]interface{}{
				`name`: `ted`,
				`age`:  7,
			},
			mapping: DefaultSqlTypeMapping,
		},
	}

	for _, expected := range tests {
		f := filter.New()

		gen := NewSqlGenerator()
		gen.Type = SqlInsertStatement
		gen.InputData = expected.input

		actual, err := filter.Render(gen, `foo`, f)
		assert.Nil(err)
		assert.Equal(expected.query, string(actual[:]))

		keys := maputil.StringKeys(expected.input)
		sort.Strings(keys)

		vv := make([]interface{}, 0)

		for _, key := range keys {
			v, _ := expected.input[key]
			vv = append(vv, v)
		}

		assert.Equal(vv, gen.GetValues())
	}
}

type updateTestData struct {
	Input  map[string]interface{}
	Filter string
}

func TestSqlUpdates(t *testing.T) {
	assert := require.New(t)

	tests := map[string]updateTestData{
		`UPDATE foo SET id = ?`: updateTestData{
			Input: map[string]interface{}{
				`id`: 1,
			},
		},
		`UPDATE foo SET name = ? WHERE (id = ?)`: updateTestData{
			Input: map[string]interface{}{
				`name`: `Bob Johnson`,
			},
			Filter: `id/1`,
		},
		`UPDATE foo SET age = ? WHERE (age < ?)`: updateTestData{
			Input: map[string]interface{}{
				`age`: 21,
			},
			Filter: `age/lt:21`,
		},
		`UPDATE foo SET enabled = ? WHERE (enabled IS NULL)`: updateTestData{
			Input: map[string]interface{}{
				`enabled`: true,
			},
			Filter: `enabled/null`,
		},
		`UPDATE foo SET enabled = ?`: updateTestData{
			Input: map[string]interface{}{
				`enabled`: nil,
			},
		},
		`UPDATE foo SET age = ?, name = ? WHERE (id = ?)`: updateTestData{
			Input: map[string]interface{}{
				`name`: `ted`,
				`age`:  7,
			},
			Filter: `id/42`,
		},
		`UPDATE foo SET age = ?, name = ? WHERE (age < ?) AND (name <> ?)`: updateTestData{
			Input: map[string]interface{}{
				`name`: `ted`,
				`age`:  7,
			},
			Filter: `age/lt:7/name/not:ted`,
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

func TestSqlDeletes(t *testing.T) {
	assert := require.New(t)

	tests := map[string]qv{
		`all`: {
			query:  `DELETE FROM foo`,
			values: []interface{}{},
		},
		`id/1`: {
			query: `DELETE FROM foo WHERE (id = ?)`,
			values: []interface{}{
				int64(1),
			},
		},
		`id/not:1`: {
			query: `DELETE FROM foo WHERE (id <> ?)`,
			values: []interface{}{
				int64(1),
			},
		},
		`name/Bob Johnson`: {
			query: `DELETE FROM foo WHERE (name = ?)`,
			values: []interface{}{
				`Bob Johnson`,
			},
		},
		`age/21`: {
			query: `DELETE FROM foo WHERE (age = ?)`,
			values: []interface{}{
				int64(21),
			},
		},
		`enabled/true`: {
			query: `DELETE FROM foo WHERE (enabled = ?)`,
			values: []interface{}{
				true,
			},
		},
		`enabled/false`: {
			query: `DELETE FROM foo WHERE (enabled = ?)`,
			values: []interface{}{
				false,
			},
		},
		`enabled/null`: {
			query: `DELETE FROM foo WHERE (enabled IS NULL)`,
			values: []interface{}{
				nil,
			},
		},
		`enabled/not:null`: {
			query: `DELETE FROM foo WHERE (enabled IS NOT NULL)`,
			values: []interface{}{
				nil,
			},
		},
		`age/lt:21`: {
			query: `DELETE FROM foo WHERE (age < ?)`,
			values: []interface{}{
				int64(21),
			},
		},
		`age/lte:21`: {
			query: `DELETE FROM foo WHERE (age <= ?)`,
			values: []interface{}{
				int64(21),
			},
		},
		`age/gt:21`: {
			query: `DELETE FROM foo WHERE (age > ?)`,
			values: []interface{}{
				int64(21),
			},
		},
		`age/gte:21`: {
			query: `DELETE FROM foo WHERE (age >= ?)`,
			values: []interface{}{
				int64(21),
			},
		},
		`factor/lt:3.141597`: {
			query:  `DELETE FROM foo WHERE (factor < ?)`,
			values: []interface{}{float64(3.141597)},
		},
		`factor/lte:3.141597`: {
			query:  `DELETE FROM foo WHERE (factor <= ?)`,
			values: []interface{}{float64(3.141597)},
		},
		`factor/gt:3.141597`: {
			query:  `DELETE FROM foo WHERE (factor > ?)`,
			values: []interface{}{float64(3.141597)},
		},
		`factor/gte:3.141597`: {
			query:  `DELETE FROM foo WHERE (factor >= ?)`,
			values: []interface{}{float64(3.141597)},
		},
		`name/contains:ob`: {
			query: `DELETE FROM foo WHERE (name LIKE ?)`,
			values: []interface{}{
				`%%ob%%`,
			},
		},
		`name/prefix:ob`: {
			query: `DELETE FROM foo WHERE (name LIKE ?)`,
			values: []interface{}{
				`ob%%`,
			},
		},
		`name/suffix:ob`: {
			query: `DELETE FROM foo WHERE (name LIKE ?)`,
			values: []interface{}{
				`%%ob`,
			},
		},
		`age/7/name/ted`: {
			query: `DELETE FROM foo WHERE (age = ?) AND (name = ?)`,
			values: []interface{}{
				int64(7),
				`ted`,
			},
		},
	}

	for spec, expected := range tests {
		f, err := filter.Parse(spec)
		assert.Nil(err)

		gen := NewSqlGenerator()
		gen.Type = SqlDeleteStatement
		actual, err := filter.Render(gen, `foo`, f)
		assert.Nil(err)
		assert.Equal(expected.query, string(actual[:]))
		assert.Equal(expected.values, gen.GetValues())
	}
}

func TestSqlPlaceholderStyles(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`age/7/name/ted/enabled/true`)
	assert.Nil(err)

	// test defaults (MySQL/sqlite compatible)
	gen := NewSqlGenerator()
	actual, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo WHERE (age = ?) AND (name = ?) AND (enabled = ?)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())

	// test PostgreSQL compatible
	gen = NewSqlGenerator()
	gen.TypeMapping = PostgresTypeMapping
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM "foo" WHERE ("age" = $1) AND ("name" = $2) AND ("enabled" = $3)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())

	// test PostgreSQL compatible
	pggen := NewSqlGenerator()
	pggen.TypeMapping = PostgresTypeMapping
	pggen.Type = SqlUpdateStatement
	pggen.InputData = map[string]interface{}{
		`created_at`: time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
		`name`:       `Tester`,
	}
	pgfilter := filter.MustParse(`id/123`)
	actual, err = filter.Render(pggen, `foo`, pgfilter)
	assert.Nil(err)
	assert.Equal(`UPDATE "foo" SET "created_at" = $1, "name" = $2 WHERE ("id" = $3)`, string(actual[:]))
	assert.Equal([]interface{}{
		time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC),
		`Tester`,
		int64(123),
	}, pggen.GetValues())

	// test Oracle compatible
	gen = NewSqlGenerator()
	gen.TypeMapping.PlaceholderFormat = `:%s`
	gen.TypeMapping.PlaceholderArgument = `field`
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo WHERE (age = :age) AND (name = :name) AND (enabled = :enabled)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())

	// test zero-indexed bracketed wacky fun placeholders
	gen = NewSqlGenerator()
	gen.TypeMapping.PlaceholderFormat = `<arg%d>`
	gen.TypeMapping.PlaceholderArgument = `index`
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo WHERE (age = <arg0>) AND (name = <arg1>) AND (enabled = <arg2>)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())
}

func TestSqlTypeMapping(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`int:age/7/str:name/ted/bool:enabled/true/float:rating/4.5/date:created_at/lt:now`)
	assert.Nil(err)

	// test default type mapping
	gen := NewSqlGenerator()
	actual, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (age = ?) `+
			`AND (name = ?) `+
			`AND (enabled = ?) `+
			`AND (rating = ?) `+
			`AND (created_at < ?)`,
		string(actual[:]),
	)

	// test null type mapping
	gen = NewSqlGenerator()
	gen.TypeMapping = DefaultSqlTypeMapping
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (age = ?) `+
			`AND (name = ?) `+
			`AND (enabled = ?) `+
			`AND (rating = ?) `+
			`AND (created_at < ?)`,
		string(actual[:]),
	)

	// test postgres type mapping
	gen = NewSqlGenerator()
	gen.TypeMapping = PostgresTypeMapping
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM "foo" `+
			`WHERE ("age" = $1) `+
			`AND ("name" = $2) `+
			`AND ("enabled" = $3) `+
			`AND ("rating" = $4) `+
			`AND ("created_at" < $5)`,
		string(actual[:]),
	)

	// test sqlite type mapping
	gen = NewSqlGenerator()
	gen.TypeMapping = SqliteTypeMapping
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM "foo" `+
			`WHERE ("age" = ?) `+
			`AND ("name" = ?) `+
			`AND ("enabled" = ?) `+
			`AND ("rating" = ?) `+
			`AND ("created_at" < ?)`,
		string(actual[:]),
	)

	// test Cassandra/CQL type mapping
	// gen = NewSqlGenerator()
	// gen.TypeMapping = CassandraTypeMapping
	// actual, err = filter.Render(gen, `foo`, f)
	// assert.Nil(err)
	// assert.Equal(
	// 	`SELECT * FROM foo `+
	// 		`WHERE (age = ?) `+
	// 		`AND (name = ?) `+
	// 		`AND (enabled = ?) `+
	// 		`AND (rating = ?) `+
	// 		`AND (created_at < ?)`,
	// 	string(actual[:]),
	// )
}

func TestSqlFieldQuoting(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`age/7/name/ted/multi field/true`)
	assert.Nil(err)

	for format, quotechar := range map[string]string{
		``:     ``,
		`%q`:   `"`,
		"`%s`": "`",
	} {
		// test default field naming
		gen := NewSqlGenerator()

		if format != `` {
			gen.TypeMapping.FieldNameFormat = format
		}

		actual, err := filter.Render(gen, `foo`, f)
		assert.Nil(err)
		assert.Equal(
			`SELECT * FROM foo `+
				`WHERE (`+quotechar+`age`+quotechar+` = ?) `+
				`AND (`+quotechar+`name`+quotechar+` = ?) `+
				`AND (`+quotechar+`multi field`+quotechar+` = ?)`,
			string(actual[:]),
		)

		// test field naming for inserts
		gen = NewSqlGenerator()

		if format != `` {
			gen.TypeMapping.FieldNameFormat = format
		}

		gen.Type = SqlInsertStatement
		gen.InputData = map[string]interface{}{
			`age`:         7,
			`name`:        `ted`,
			`multi field`: true,
		}

		actual, err = filter.Render(gen, `foo`, f)
		assert.Nil(err)
		assert.Equal(
			`INSERT INTO foo (`+quotechar+`age`+quotechar+`, `+
				quotechar+`multi field`+quotechar+`, `+
				quotechar+`name`+quotechar+`) VALUES (?, ?, ?)`,
			string(actual[:]),
		)

		// test field naming for updates
		gen = NewSqlGenerator()

		if format != `` {
			gen.TypeMapping.FieldNameFormat = format
		}

		gen.Type = SqlUpdateStatement
		gen.InputData = map[string]interface{}{
			`age`:         7,
			`name`:        `ted`,
			`multi field`: true,
		}

		actual, err = filter.Render(gen, `foo`, f)
		assert.Nil(err)
		assert.Equal(
			`UPDATE foo SET `+
				quotechar+`age`+quotechar+` = ?, `+
				quotechar+`multi field`+quotechar+` = ?, `+
				quotechar+`name`+quotechar+` = ? `+
				`WHERE (`+quotechar+`age`+quotechar+` = ?) `+
				`AND (`+quotechar+`name`+quotechar+` = ?) `+
				`AND (`+quotechar+`multi field`+quotechar+` = ?)`,
			string(actual[:]),
		)
	}
}

func TestSqlMultipleValues(t *testing.T) {
	assert := require.New(t)

	fn := func(tests map[string]qv, withIn bool) {
		for spec, expected := range tests {
			f, err := filter.Parse(spec)
			assert.Nil(err)

			gen := NewSqlGenerator()
			gen.UseInStatement = withIn

			actual, err := filter.Render(gen, `foo`, f)
			assert.Nil(err)
			assert.Equal(expected.query, string(actual[:]))
			assert.Equal(expected.values, gen.GetValues())
		}
	}

	fn(map[string]qv{
		`id/1`: {
			query: `SELECT * FROM foo WHERE (id = ?)`,
			values: []interface{}{
				int64(1),
			},
		},
		`id/1|2`: {
			query: `SELECT * FROM foo WHERE (id IN(?, ?))`,
			values: []interface{}{
				int64(1),
				int64(2),
			},
		},
		`id/1|2|3`: {
			query: `SELECT * FROM foo WHERE (id IN(?, ?, ?))`,
			values: []interface{}{
				int64(1),
				int64(2),
				int64(3),
			},
		},
		`id/1|2|3/age/7`: {
			query: `SELECT * FROM foo WHERE (id IN(?, ?, ?)) AND (age = ?)`,
			values: []interface{}{
				int64(1),
				int64(2),
				int64(3),
				int64(7),
			},
		},
	}, true)

	fn(map[string]qv{
		`id/1`: {
			query: `SELECT * FROM foo WHERE (id = ?)`,
			values: []interface{}{
				int64(1),
			},
		},
		`id/1|2`: {
			query: `SELECT * FROM foo WHERE (id = ? OR id = ?)`,
			values: []interface{}{
				int64(1),
				int64(2),
			},
		},
		`id/1|2|3`: {
			query: `SELECT * FROM foo WHERE (id = ? OR id = ? OR id = ?)`,
			values: []interface{}{
				int64(1),
				int64(2),
				int64(3),
			},
		},
		`id/1|2|3/age/7`: {
			query: `SELECT * FROM foo WHERE (id = ? OR id = ? OR id = ?) AND (age = ?)`,
			values: []interface{}{
				int64(1),
				int64(2),
				int64(3),
				int64(7),
			},
		},
	}, false)
}

func TestSqlMultipleValuesWithNormalizer(t *testing.T) {
	assert := require.New(t)

	fn := func(tests map[string]qv, withIn bool) {
		for spec, expected := range tests {
			f, err := filter.Parse(spec)
			assert.Nil(err)

			gen := NewSqlGenerator()
			gen.UseInStatement = withIn
			gen.NormalizeFields = []string{`id`}
			gen.NormalizerFormat = `LOWER(%v)`

			actual, err := filter.Render(gen, `foo`, f)
			assert.Nil(err)
			assert.Equal(expected.query, string(actual[:]))
			assert.Equal(expected.values, gen.GetValues())
		}
	}

	fn(map[string]qv{
		`id/1`: {
			query: `SELECT * FROM foo WHERE (id = ?)`,
			values: []interface{}{
				int64(1),
			},
		},
		`id/1|2`: {
			query: `SELECT * FROM foo WHERE (id IN(?, ?))`,
			values: []interface{}{
				int64(1),
				int64(2),
			},
		},
		`id/1|2|3`: {
			query: `SELECT * FROM foo WHERE (id IN(?, ?, ?))`,
			values: []interface{}{
				int64(1),
				int64(2),
				int64(3),
			},
		},
		`id/1|2|3/age/7`: {
			query: `SELECT * FROM foo WHERE (id IN(?, ?, ?)) AND (age = ?)`,
			values: []interface{}{
				int64(1),
				int64(2),
				int64(3),
				int64(7),
			},
		},
	}, true)
}

func TestSqlSorting(t *testing.T) {
	assert := require.New(t)

	f := filter.All()
	f.Sort = []string{`+name`, `-age`}

	gen := NewSqlGenerator()

	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)

	assert.Equal(`SELECT * FROM foo ORDER BY name ASC, age DESC`, string(sql[:]))
}

func TestSqlLimitOffset(t *testing.T) {
	assert := require.New(t)

	f := filter.All()
	f.Limit = 4
	gen := NewSqlGenerator()
	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo LIMIT 4`, string(sql[:]))

	f = filter.All()
	f.Limit = 4
	f.Offset = 12
	gen = NewSqlGenerator()
	sql, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo LIMIT 4 OFFSET 12`, string(sql[:]))
}

func TestSqlSelectFull(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`+name/prefix:ted/-age/gt:7/city/suffix:berg/state/contains:new`)
	assert.Nil(err)
	f.Limit = 4
	f.Offset = 12
	f.Fields = []string{`id`, `age`}

	gen := NewSqlGenerator()
	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)

	assert.Equal(
		`SELECT id, age FROM foo `+
			`WHERE (name LIKE ?) `+
			`AND (age > ?) `+
			`AND (city LIKE ?) `+
			`AND (state LIKE ?) `+
			`ORDER BY name ASC, age DESC `+
			`LIMIT 4 OFFSET 12`,
		string(sql[:]),
	)

	assert.Equal([]interface{}{
		`ted%%`,
		int64(7),
		`%%berg`,
		`%%new%%`,
	}, gen.GetValues())
}

func TestSqlSelectWithNormalizerAndPlaceholders(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`+name/prefix:ted/-age/gt:7/city/suffix:berg/state/contains:new`)
	assert.Nil(err)
	f.Limit = 4
	f.Offset = 12
	f.Fields = []string{`id`, `age`}

	gen := NewSqlGenerator()
	gen.NormalizeFields = []string{`name`, `city`}
	gen.NormalizerFormat = `LOWER(%s)`
	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)

	assert.Equal(
		`SELECT id, age FROM foo `+
			`WHERE (LOWER(name) LIKE LOWER(?)) `+
			`AND (age > ?) `+
			`AND (LOWER(city) LIKE LOWER(?)) `+
			`AND (state LIKE ?) `+
			`ORDER BY name ASC, age DESC `+
			`LIMIT 4 OFFSET 12`,
		string(sql[:]),
	)

	assert.Equal([]interface{}{
		`ted%%`,
		int64(7),
		`%%berg`,
		`%%new%%`,
	}, gen.GetValues())
}

func TestSqlSelectAggregateFunctions(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`+name/prefix:ted/city/suffix:berg/state/contains:new`)
	assert.Nil(err)
	f.Limit = 4
	f.Offset = 12
	f.Fields = []string{`age`}

	gen := NewSqlGenerator()
	gen.FieldWrappers = map[string]string{
		`age`: "SUM(%s)",
	}

	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)

	assert.Equal(
		`SELECT SUM(age) FROM foo `+
			`WHERE (name LIKE ?) `+
			`AND (city LIKE ?) `+
			`AND (state LIKE ?) `+
			`ORDER BY name ASC `+
			`LIMIT 4 OFFSET 12`,
		string(sql[:]),
	)

	assert.Equal([]interface{}{
		`ted%%`,
		`%%berg`,
		`%%new%%`,
	}, gen.GetValues())
}

func TestSqlSelectGroupBy(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`all`)
	assert.Nil(err)
	f.Fields = []string{`state`, `city`}

	gen := NewSqlGenerator()

	gen.GroupByField(`state`)
	gen.GroupByField(`city`)
	gen.AggregateByField(filter.Average, `age`)

	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)

	assert.Equal(
		`SELECT state, city, AVG(age) AS age FROM foo GROUP BY state, city`,
		string(sql[:]),
	)
}

func TestSqlBulkDelete(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`name/not:Bob|Frank|Steve`)
	assert.Nil(err)

	gen := NewSqlGenerator()
	gen.Type = SqlDeleteStatement

	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)

	assert.Equal(
		`DELETE FROM foo WHERE (name NOT IN(?, ?, ?))`,
		string(sql[:]),
	)

	assert.Equal([]interface{}{
		`Bob`,
		`Frank`,
		`Steve`,
	}, gen.GetValues())
}

func TestSqlBulkDeleteWithNormalizers(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`name/unlike:Bob|Frank|Steve`)
	assert.Nil(err)

	gen := NewSqlGenerator()
	gen.Type = SqlDeleteStatement
	gen.NormalizeFields = []string{`name`}
	gen.NormalizerFormat = `LOWER(%s)`

	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)

	assert.Equal(
		`DELETE FROM foo WHERE (LOWER(name) NOT IN(LOWER(?), LOWER(?), LOWER(?)))`,
		string(sql[:]),
	)

	assert.Equal([]interface{}{
		`Bob`,
		`Frank`,
		`Steve`,
	}, gen.GetValues())
}

func TestSqlToNativeType(t *testing.T) {
	assert := require.New(t)

	type tests struct {
		Mapping  SqlTypeMapping
		Type     dal.Type
		Subtypes []dal.Type
		Length   int
		Expected string
	}

	for i, tcase := range []tests{
		{SqliteTypeMapping, dal.StringType, nil, 0, `TEXT`},
		{SqliteTypeMapping, dal.StringType, nil, 42, `TEXT(42)`},
		{SqliteTypeMapping, dal.IntType, nil, 0, `INTEGER`},
		{SqliteTypeMapping, dal.IntType, nil, 14, `INTEGER(14)`},
		{SqliteTypeMapping, dal.FloatType, nil, 0, `REAL`},
		{SqliteTypeMapping, dal.FloatType, nil, 5, `REAL(5)`},
		{SqliteTypeMapping, dal.BooleanType, nil, 0, `INTEGER(1)`},
		{SqliteTypeMapping, dal.BooleanType, nil, 4, `INTEGER(1)`},
		{SqliteTypeMapping, dal.TimeType, nil, 0, `INTEGER`},
		{SqliteTypeMapping, dal.ObjectType, []dal.Type{dal.StringType, dal.AutoType}, 0, `BLOB`},
		{SqliteTypeMapping, dal.ObjectType, []dal.Type{dal.StringType, dal.AutoType}, 123456, `BLOB(123456)`},
		{SqliteTypeMapping, dal.ArrayType, []dal.Type{dal.IntType}, 0, `BLOB`},
		{SqliteTypeMapping, dal.ArrayType, []dal.Type{dal.IntType}, 4321, `BLOB(4321)`},
		{SqliteTypeMapping, dal.RawType, nil, 0, `BLOB`},
		{SqliteTypeMapping, dal.RawType, nil, 256, `BLOB(256)`},
		{MysqlTypeMapping, dal.StringType, nil, 0, `VARCHAR(255)`},
		{MysqlTypeMapping, dal.StringType, nil, 42, `VARCHAR(42)`},
		{MysqlTypeMapping, dal.IntType, nil, 0, `BIGINT`},
		{MysqlTypeMapping, dal.IntType, nil, 14, `BIGINT(14)`},
		{MysqlTypeMapping, dal.FloatType, nil, 0, `DECIMAL(10,8)`},
		{MysqlTypeMapping, dal.FloatType, nil, 5, `DECIMAL(5,8)`},
		{MysqlTypeMapping, dal.BooleanType, nil, 0, `BOOL`},
		{MysqlTypeMapping, dal.TimeType, nil, 0, `DATETIME`},
		{MysqlTypeMapping, dal.ObjectType, []dal.Type{dal.StringType, dal.AutoType}, 0, `MEDIUMBLOB`},
		{MysqlTypeMapping, dal.ObjectType, []dal.Type{dal.StringType, dal.AutoType}, 123456, `MEDIUMBLOB(123456)`},
		{MysqlTypeMapping, dal.ArrayType, []dal.Type{dal.IntType}, 0, `MEDIUMBLOB`},
		{MysqlTypeMapping, dal.ArrayType, []dal.Type{dal.IntType}, 4321, `MEDIUMBLOB(4321)`},
		{MysqlTypeMapping, dal.RawType, nil, 0, `MEDIUMBLOB`},
		{MysqlTypeMapping, dal.RawType, nil, 256, `MEDIUMBLOB(256)`},
		{PostgresTypeMapping, dal.StringType, nil, 0, `TEXT`},
		{PostgresTypeMapping, dal.StringType, nil, 42, `TEXT(42)`},
		{PostgresTypeMapping, dal.IntType, nil, 0, `BIGINT`},
		{PostgresTypeMapping, dal.IntType, nil, 14, `BIGINT(14)`},
		{PostgresTypeMapping, dal.FloatType, nil, 0, `NUMERIC`},
		{PostgresTypeMapping, dal.FloatType, nil, 5, `NUMERIC(5)`},
		{PostgresTypeMapping, dal.BooleanType, nil, 0, `BOOLEAN`},
		{PostgresTypeMapping, dal.TimeType, nil, 0, `TIMESTAMP`},
		{PostgresTypeMapping, dal.ObjectType, []dal.Type{dal.StringType, dal.AutoType}, 0, `VARCHAR`},
		{PostgresTypeMapping, dal.ObjectType, []dal.Type{dal.StringType, dal.AutoType}, 123456, `VARCHAR(123456)`},
		{PostgresTypeMapping, dal.ArrayType, []dal.Type{dal.IntType}, 0, `VARCHAR`},
		{PostgresTypeMapping, dal.ArrayType, []dal.Type{dal.IntType}, 4321, `VARCHAR(4321)`},
		{PostgresTypeMapping, dal.RawType, nil, 0, `BYTEA`},
		{PostgresTypeMapping, dal.RawType, nil, 256, `BYTEA(256)`},
		{CassandraTypeMapping, dal.StringType, nil, 0, `VARCHAR`},
		{CassandraTypeMapping, dal.StringType, nil, 42, `VARCHAR(42)`},
		{CassandraTypeMapping, dal.IntType, nil, 0, `INT`},
		{CassandraTypeMapping, dal.IntType, nil, 14, `INT(14)`},
		{CassandraTypeMapping, dal.FloatType, nil, 0, `FLOAT`},
		{CassandraTypeMapping, dal.FloatType, nil, 5, `FLOAT(5)`},
		{CassandraTypeMapping, dal.BooleanType, nil, 0, `TINYINT(1)`},
		{CassandraTypeMapping, dal.TimeType, nil, 0, `DATETIME`},
		{CassandraTypeMapping, dal.ObjectType, []dal.Type{dal.StringType, dal.IntType}, 0, `MAP<VARCHAR,INT>`},
		{CassandraTypeMapping, dal.ObjectType, []dal.Type{dal.StringType, dal.BooleanType}, 0, `MAP<VARCHAR,TINYINT(1)>`},
		{CassandraTypeMapping, dal.ArrayType, []dal.Type{dal.IntType}, 0, `LIST<INT>`},
		{CassandraTypeMapping, dal.RawType, nil, 0, `BLOB`},
		{CassandraTypeMapping, dal.RawType, nil, 256, `BLOB(256)`},
	} {
		help := fmt.Sprintf("Case %d: %v %v(%v)", i, tcase.Mapping, tcase.Type, tcase.Subtypes)
		gen := NewSqlGenerator()
		gen.TypeMapping = tcase.Mapping

		typ, err := gen.ToNativeType(tcase.Type, tcase.Subtypes, tcase.Length)
		assert.NoError(err, help)
		assert.Equal(tcase.Expected, typ, help)
	}
}
