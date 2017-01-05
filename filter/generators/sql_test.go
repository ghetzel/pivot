package generators

import (
	"github.com/ghetzel/pivot/filter"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

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
		`UPDATE foo SET id = 1`: updateTestData{
			Input: map[string]interface{}{
				`id`: 1,
			},
		},
		`UPDATE foo SET name = 'Bob Johnson' WHERE (id = 1)`: updateTestData{
			Input: map[string]interface{}{
				`name`: `Bob Johnson`,
			},
			Filter: `id/1`,
		},
		`UPDATE foo SET age = 21 WHERE (age < 21)`: updateTestData{
			Input: map[string]interface{}{
				`age`: 21,
			},
			Filter: `age/lt:21`,
		},
		`UPDATE foo SET enabled = true WHERE (enabled IS NULL)`: updateTestData{
			Input: map[string]interface{}{
				`enabled`: true,
			},
			Filter: `enabled/null`,
		},
		`UPDATE foo SET enabled = false`: updateTestData{
			Input: map[string]interface{}{
				`enabled`: false,
			},
		},
		`UPDATE foo SET enabled = NULL`: updateTestData{
			Input: map[string]interface{}{
				`enabled`: nil,
			},
		},
		`UPDATE foo SET age = 7, name = 'ted' WHERE (id = 42)`: updateTestData{
			Input: map[string]interface{}{
				`name`: `ted`,
				`age`:  7,
			},
			Filter: `id/42`,
		},
		`UPDATE foo SET age = 7, name = 'ted' WHERE (age < 7) AND (name <> 'ted')`: updateTestData{
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

	tests := map[string]string{
		`all`:              `DELETE FROM foo`,
		`id/1`:             `DELETE FROM foo WHERE (id = 1)`,
		`id/not:1`:         `DELETE FROM foo WHERE (id <> 1)`,
		`name/Bob Johnson`: `DELETE FROM foo WHERE (name = 'Bob Johnson')`,
		`age/21`:           `DELETE FROM foo WHERE (age = 21)`,
		`int:age/21`:       `DELETE FROM foo WHERE (CAST(age AS BIGINT) = 21)`,
		`float:age/21`:     `DELETE FROM foo WHERE (CAST(age AS DECIMAL) = 21)`,
		`enabled/true`:     `DELETE FROM foo WHERE (enabled = true)`,
		`enabled/false`:    `DELETE FROM foo WHERE (enabled = false)`,
		`enabled/null`:     `DELETE FROM foo WHERE (enabled IS NULL)`,
		`enabled/not:null`: `DELETE FROM foo WHERE (enabled IS NOT NULL)`,
		`age/lt:21`:        `DELETE FROM foo WHERE (age < 21)`,
		`age/lte:21`:       `DELETE FROM foo WHERE (age <= 21)`,
		`age/gt:21`:        `DELETE FROM foo WHERE (age > 21)`,
		`age/gte:21`:       `DELETE FROM foo WHERE (age >= 21)`,
		`name/contains:ob`: `DELETE FROM foo WHERE (name LIKE '%%ob%%')`,
		`name/prefix:ob`:   `DELETE FROM foo WHERE (name LIKE 'ob%%')`,
		`name/suffix:ob`:   `DELETE FROM foo WHERE (name LIKE '%%ob')`,
		`age/7/name/ted`:   `DELETE FROM foo WHERE (age = 7) AND (name = 'ted')`,
	}

	for spec, expected := range tests {
		f, err := filter.Parse(spec)
		assert.Nil(err)

		gen := NewSqlGenerator()
		gen.Type = SqlDeleteStatement
		actual, err := filter.Render(gen, `foo`, f)
		assert.Nil(err)
		assert.Equal(expected, string(actual[:]))
	}
}

func TestSqlPlaceholderStyles(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`age/7/name/ted/enabled/true`)
	assert.Nil(err)

	// test defaults (MySQL/sqlite compatible)
	gen := NewSqlGenerator()
	gen.UsePlaceholders = true
	actual, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo WHERE (age = ?) AND (name = ?) AND (enabled = ?)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())

	// test PostgreSQL compatible
	gen = NewSqlGenerator()
	gen.UsePlaceholders = true
	gen.PlaceholderFormat = `$%d`
	gen.PlaceholderArgument = `index1`
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo WHERE (age = $1) AND (name = $2) AND (enabled = $3)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())

	// test Oracle compatible
	gen = NewSqlGenerator()
	gen.UsePlaceholders = true
	gen.PlaceholderFormat = `:%s`
	gen.PlaceholderArgument = `field`
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(`SELECT * FROM foo WHERE (age = :age) AND (name = :name) AND (enabled = :enabled)`, string(actual[:]))
	assert.Equal([]interface{}{int64(7), `ted`, true}, gen.GetValues())

	// test zero-indexed bracketed wacky fun placeholders
	gen = NewSqlGenerator()
	gen.UsePlaceholders = true
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
	gen.UsePlaceholders = true
	actual, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (CAST(age AS BIGINT) = ?) `+
			`AND (CAST(name AS VARCHAR(255)) = ?) `+
			`AND (CAST(enabled AS BOOL) = ?) `+
			`AND (CAST(rating AS DECIMAL) = ?) `+
			`AND (CAST(created_at AS DATETIME) < ?)`,
		string(actual[:]),
	)

	// test null type mapping
	gen = NewSqlGenerator()
	gen.UsePlaceholders = true
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
	gen.UsePlaceholders = true
	gen.TypeMapping = PostgresTypeMapping
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (CAST(age AS BIGINT) = ?) `+
			`AND (CAST(name AS TEXT) = ?) `+
			`AND (CAST(enabled AS BOOLEAN) = ?) `+
			`AND (CAST(rating AS NUMERIC) = ?) `+
			`AND (CAST(created_at AS TIMESTAMP) < ?)`,
		string(actual[:]),
	)

	// test sqlite type mapping
	gen = NewSqlGenerator()
	gen.UsePlaceholders = true
	gen.TypeMapping = SqliteTypeMapping
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (CAST(age AS INTEGER) = ?) `+
			`AND (CAST(name AS TEXT) = ?) `+
			`AND (CAST(enabled AS INTEGER(1)) = ?) `+
			`AND (CAST(rating AS REAL) = ?) `+
			`AND (CAST(created_at AS INTEGER) < ?)`,
		string(actual[:]),
	)

	// test Cassandra/CQL type mapping
	gen = NewSqlGenerator()
	gen.UsePlaceholders = true
	gen.TypeMapping = CassandraTypeMapping
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (CAST(age AS INT) = ?) `+
			`AND (CAST(name AS VARCHAR) = ?) `+
			`AND (CAST(enabled AS TINYINT(1)) = ?) `+
			`AND (CAST(rating AS FLOAT) = ?) `+
			`AND (CAST(created_at AS DATETIME) < ?)`,
		string(actual[:]),
	)
}

func TestSqlQuoting(t *testing.T) {
	assert := require.New(t)

	f, err := filter.Parse(`name/Bob Johnson/str:num/42/num1/42`)
	assert.Nil(err)

	// test default quoting
	gen := NewSqlGenerator()
	gen.TypeMapping = NoTypeMapping
	actual, err := filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (name = 'Bob Johnson') `+
			`AND (num = '42') `+
			`AND (num1 = 42)`,
		string(actual[:]),
	)

	// test backtick quoting
	gen = NewSqlGenerator()
	gen.TypeMapping = NoTypeMapping
	gen.QuotedValueFormat = "`%s`"
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			"WHERE (name = `Bob Johnson`) "+
			"AND (num = `42`) "+
			`AND (num1 = 42)`,
		string(actual[:]),
	)

	// test double quoting
	gen = NewSqlGenerator()
	gen.TypeMapping = NoTypeMapping
	gen.QuotedValueFormat = "%q"
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (name = "Bob Johnson") `+
			`AND (num = "42") `+
			`AND (num1 = 42)`,
		string(actual[:]),
	)

	// test weird treatment of unquoted values
	gen = NewSqlGenerator()
	gen.TypeMapping = NoTypeMapping
	gen.UnquotedValueFormat = "<%v>"
	actual, err = filter.Render(gen, `foo`, f)
	assert.Nil(err)
	assert.Equal(
		`SELECT * FROM foo `+
			`WHERE (name = 'Bob Johnson') `+
			`AND (num = '42') `+
			`AND (num1 = <42>)`,
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
		gen.UsePlaceholders = true

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
		gen.UsePlaceholders = true

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
		gen.UsePlaceholders = true

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

	fn := func(tests map[string]string, withIn bool) {
		for spec, expected := range tests {
			f, err := filter.Parse(spec)
			assert.Nil(err)

			gen := NewSqlGenerator()
			gen.UseInStatement = withIn

			actual, err := filter.Render(gen, `foo`, f)
			assert.Nil(err)
			assert.Equal(expected, string(actual[:]))
		}
	}

	fn(map[string]string{
		`id/1`:           `SELECT * FROM foo WHERE (id = 1)`,
		`id/1|2`:         `SELECT * FROM foo WHERE (id IN(1, 2))`,
		`id/1|2|3`:       `SELECT * FROM foo WHERE (id IN(1, 2, 3))`,
		`id/1|2|3/age/7`: `SELECT * FROM foo WHERE (id IN(1, 2, 3)) AND (age = 7)`,
	}, true)

	fn(map[string]string{
		`id/1`:           `SELECT * FROM foo WHERE (id = 1)`,
		`id/1|2`:         `SELECT * FROM foo WHERE (id = 1 OR id = 2)`,
		`id/1|2|3`:       `SELECT * FROM foo WHERE (id = 1 OR id = 2 OR id = 3)`,
		`id/1|2|3/age/7`: `SELECT * FROM foo WHERE (id = 1 OR id = 2 OR id = 3) AND (age = 7)`,
	}, false)
}
