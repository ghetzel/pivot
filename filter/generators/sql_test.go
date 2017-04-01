package generators

import (
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/filter"
	"github.com/stretchr/testify/require"
	"sort"
	"strings"
	"testing"
)

type qv struct {
	query  string
	values []interface{}
	input  map[string]interface{}
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
				query:  `SELECT ` + field + ` FROM foo`,
				values: []interface{}{},
			},
			`id/1`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (id = ?)`,
				values: []interface{}{int64(1)},
			},
			`id/not:1`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (id <> ?)`,
				values: []interface{}{int64(1)},
			},
			`name/Bob Johnson`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (name = ?)`,
				values: []interface{}{`Bob Johnson`},
			},
			`age/21`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (age = ?)`,
				values: []interface{}{int64(21)},
			},
			`enabled/true`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (enabled = ?)`,
				values: []interface{}{true},
			},
			`enabled/false`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (enabled = ?)`,
				values: []interface{}{false},
			},
			`enabled/null`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (enabled IS NULL)`,
				values: []interface{}{nil},
			},
			`enabled/not:null`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (enabled IS NOT NULL)`,
				values: []interface{}{nil},
			},
			`age/lt:21`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (age < ?)`,
				values: []interface{}{int64(21)},
			},
			`age/lte:21`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (age <= ?)`,
				values: []interface{}{int64(21)},
			},
			`age/gt:21`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (age > ?)`,
				values: []interface{}{int64(21)},
			},
			`age/gte:21`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (age >= ?)`,
				values: []interface{}{int64(21)},
			},
			`factor/lt:3.141597`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (factor < ?)`,
				values: []interface{}{float64(3.141597)},
			},
			`factor/lte:3.141597`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (factor <= ?)`,
				values: []interface{}{float64(3.141597)},
			},
			`factor/gt:3.141597`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (factor > ?)`,
				values: []interface{}{float64(3.141597)},
			},
			`factor/gte:3.141597`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (factor >= ?)`,
				values: []interface{}{float64(3.141597)},
			},
			`name/contains:ob`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (name LIKE ?)`,
				values: []interface{}{`%%ob%%`},
			},
			`name/prefix:ob`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (name LIKE ?)`,
				values: []interface{}{`ob%%`},
			},
			`name/suffix:ob`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (name LIKE ?)`,
				values: []interface{}{`%%ob`},
			},
			`age/7/name/ted`: {
				query:  `SELECT ` + field + ` FROM foo WHERE (age = ?) AND (name = ?)`,
				values: []interface{}{int64(7), `ted`},
			},
		}

		for spec, expected := range tests {
			f, err := filter.Parse(spec)
			assert.Nil(err)
			if field != `*` {
				f.Fields = strings.Split(field, `, `)
			}

			gen := NewSqlGenerator()
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
			`INSERT INTO foo (id) VALUES (?)`,
			nil,
			map[string]interface{}{
				`id`: 1,
			},
		}, {
			`INSERT INTO foo (name) VALUES (?)`,
			nil,
			map[string]interface{}{
				`name`: `Bob Johnson`,
			},
		}, {
			`INSERT INTO foo (age) VALUES (?)`,
			nil,
			map[string]interface{}{
				`age`: 21,
			},
		}, {
			`INSERT INTO foo (enabled) VALUES (?)`,
			nil,
			map[string]interface{}{
				`enabled`: true,
			},
		}, {
			`INSERT INTO foo (enabled) VALUES (?)`,
			nil,
			map[string]interface{}{
				`enabled`: false,
			},
		}, {
			`INSERT INTO foo (enabled) VALUES (?)`,
			nil,
			map[string]interface{}{
				`enabled`: nil,
			},
		}, {
			`INSERT INTO foo (age, name) VALUES (?, ?)`,
			nil,
			map[string]interface{}{
				`name`: `ted`,
				`age`:  7,
			},
		},
	}

	for _, expected := range tests {
		f := filter.MakeFilter(``)

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
	gen.PlaceholderFormat = `$%d`
	gen.PlaceholderArgument = `index1`
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo WHERE (age = $1) AND (name = $2) AND (enabled = $3)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())

	// test Oracle compatible
	gen = NewSqlGenerator()
	gen.PlaceholderFormat = `:%s`
	gen.PlaceholderArgument = `field`
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo WHERE (age = :age) AND (name = :name) AND (enabled = :enabled)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())

	// test zero-indexed bracketed wacky fun placeholders
	gen = NewSqlGenerator()
	gen.PlaceholderFormat = `<arg%d>`
	gen.PlaceholderArgument = `index`
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
	gen.TypeMapping = NoTypeMapping
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
		`SELECT * FROM foo `+
			`WHERE (age = ?) `+
			`AND (name = ?) `+
			`AND (enabled = ?) `+
			`AND (rating = ?) `+
			`AND (created_at < ?)`,
		string(actual[:]),
	)

	// test sqlite type mapping
	gen = NewSqlGenerator()
	gen.TypeMapping = SqliteTypeMapping
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

	// test Cassandra/CQL type mapping
	gen = NewSqlGenerator()
	gen.TypeMapping = CassandraTypeMapping
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
			gen.FieldNameFormat = format
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
			gen.FieldNameFormat = format
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
			gen.FieldNameFormat = format
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

func TestSqlSorting(t *testing.T) {
	assert := require.New(t)

	f := filter.MakeFilter(`all`)
	f.Sort = []string{`+name`, `-age`}

	gen := NewSqlGenerator()

	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)

	assert.Equal(`SELECT * FROM foo ORDER BY name ASC, age DESC`, string(sql[:]))
}

func TestSqlLimitOffset(t *testing.T) {
	assert := require.New(t)

	f := filter.MakeFilter(`all`)
	f.Limit = 4
	gen := NewSqlGenerator()
	sql, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo LIMIT 4`, string(sql[:]))

	f = filter.MakeFilter(`all`)
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
