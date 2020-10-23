package filter

import (
	"testing"

	"github.com/ghetzel/pivot/v3/dal"
	"github.com/stretchr/testify/require"
)

func TestFilterParse(t *testing.T) {
	assert := require.New(t)

	CriteriaSeparator = `/`
	FieldTermSeparator = `/`

	tests := map[string]func(*Filter, error){
		AllValue: func(f *Filter, err error) {
			assert.NoError(err)
			assert.True(f.MatchAll)
			assert.Equal(f.Spec, AllValue)
			assert.Equal(0, len(f.Criteria))
		},
		`k1/contains:v1/int:k2/lt:v2a|v2b`: func(f *Filter, err error) {
			assert.NoError(err)
			assert.Equal(2, len(f.Criteria))

			assert.True(dal.AutoType == f.Criteria[0].Type)
			assert.Equal(`k1`, f.Criteria[0].Field)
			assert.Equal(`contains`, f.Criteria[0].Operator)
			assert.Equal([]interface{}{`v1`}, f.Criteria[0].Values)

			assert.True(dal.IntType == f.Criteria[1].Type)
			assert.Equal(`k2`, f.Criteria[1].Field)
			assert.Equal(`lt`, f.Criteria[1].Operator)
			assert.Equal([]interface{}{`v2a`, `v2b`}, f.Criteria[1].Values)
		},
	}

	for spec, fn := range tests {
		f, err := Parse(spec)
		fn(f, err)
	}
}

func TestFilterIdentity(t *testing.T) {
	assert := require.New(t)
	spec := `str#16:name/prefix:foo`

	filter, err := Parse(spec)
	assert.NoError(err)
	assert.Equal(1, len(filter.Criteria))
	assert.Equal(dal.StringType, filter.Criteria[0].Type)
	assert.Equal(16, filter.Criteria[0].Length)
	assert.Equal(`name`, filter.Criteria[0].Field)
	assert.Equal(`prefix`, filter.Criteria[0].Operator)
	assert.Equal([]interface{}{`foo`}, filter.Criteria[0].Values)

	assert.Equal(spec, filter.String())
}

func TestFilterIdOnly(t *testing.T) {
	assert := require.New(t)

	f := MakeFilter(AllValue)
	f.Fields = []string{f.IdentityField}
	assert.True(f.IdOnly())

	f = MakeFilter(AllValue)
	assert.False(f.IdOnly())
}

func TestFilterParseAltDelimiters(t *testing.T) {
	assert := require.New(t)

	cs, fts := CriteriaSeparator, FieldTermSeparator
	CriteriaSeparator = ` `
	FieldTermSeparator = `=`

	tests := map[string]func(*Filter, error){
		AllValue: func(f *Filter, err error) {
			assert.NoError(err)
			assert.True(f.MatchAll)
			assert.Equal(f.Spec, AllValue)
			assert.Equal(0, len(f.Criteria))
		},
		`k1=contains:v1 int:k2=lt:v2a|v2b`: func(f *Filter, err error) {
			assert.NoError(err)
			assert.Equal(2, len(f.Criteria))

			assert.True(dal.AutoType == f.Criteria[0].Type)
			assert.Equal(`k1`, f.Criteria[0].Field)
			assert.Equal(`contains`, f.Criteria[0].Operator)
			assert.Equal([]interface{}{`v1`}, f.Criteria[0].Values)

			assert.True(dal.IntType == f.Criteria[1].Type)
			assert.Equal(`k2`, f.Criteria[1].Field)
			assert.Equal(`lt`, f.Criteria[1].Operator)
			assert.Equal([]interface{}{`v2a`, `v2b`}, f.Criteria[1].Values)
		},
	}

	for spec, fn := range tests {
		f, err := Parse(spec)
		fn(f, err)
	}

	// reset these
	CriteriaSeparator, FieldTermSeparator = cs, fts
}

func TestFilterFromMap(t *testing.T) {
	assert := require.New(t)

	f, err := FromMap(map[string]interface{}{
		`f1`:       `v1`,
		`int:f2`:   2,
		`float:f3`: `gte:3`,
		`id`:       []string{`1`, `3`, `5`},
		`other`:    `2|4|6`,
	})

	assert.NoError(err)
	assert.Equal(5, len(f.Criteria))

	for _, criterion := range f.Criteria {
		switch criterion.Field {
		case `f1`:
			assert.Equal([]interface{}{`v1`}, criterion.Values)

		case `f2`:
			assert.True(dal.IntType == criterion.Type)
			assert.Equal([]interface{}{2}, criterion.Values)

		case `f3`:
			assert.True(dal.FloatType == criterion.Type)
			assert.Equal(`gte`, criterion.Operator)
			assert.Equal([]interface{}{`3`}, criterion.Values)

		case `id`:
			assert.EqualValues([]interface{}{`1`, `3`, `5`}, criterion.Values)

		case `other`:
			assert.EqualValues([]interface{}{`2`, `4`, `6`}, criterion.Values)
		default:
			t.Errorf("Unknown field %q", criterion.Field)
		}
	}
}

func TestFilterGetSort(t *testing.T) {
	assert := require.New(t)

	f, err := Parse(`name/test/-age/4/+group/one`)
	assert.NoError(err)

	sortBy := f.GetSort()

	assert.Equal(2, len(sortBy))

	assert.Equal(`age`, sortBy[0].Field)
	assert.True(sortBy[0].Descending)

	assert.Equal(`group`, sortBy[1].Field)
	assert.False(sortBy[1].Descending)
}

func TestFilterCopy(t *testing.T) {
	assert := require.New(t)

	f1, err := Parse(`id/42`)
	assert.NoError(err)

	f2 := Copy(f1)

	assert.Equal([]Criterion{
		{
			Type:   dal.AutoType,
			Field:  `id`,
			Values: []interface{}{`42`},
		},
	}, f1.Criteria)

	f2.AddCriteria(Criterion{
		Type:   dal.StringType,
		Field:  `name`,
		Values: []interface{}{`test`},
	})

	assert.Equal([]Criterion{
		{
			Type:   dal.AutoType,
			Field:  `id`,
			Values: []interface{}{`42`},
		},
	}, f1.Criteria)

	assert.Equal([]Criterion{
		{
			Type:   dal.AutoType,
			Field:  `id`,
			Values: []interface{}{`42`},
		}, {
			Type:   dal.StringType,
			Field:  `name`,
			Values: []interface{}{`test`},
		},
	}, f2.Criteria)
}

func TestFilterParseStruct(t *testing.T) {
	assert := require.New(t)

	type fCoolObject struct {
		Name    string `pivot:"name"`
		Enabled bool
		Age     int `pivot:",wwuuuuuut"`
	}

	f, err := Parse(fCoolObject{
		Name:    `test1`,
		Enabled: true,
		Age:     42,
	})

	assert.NoError(err)
	assert.Equal(3, len(f.Criteria))

	values, ok := f.GetValues(`name`)
	assert.True(ok)
	assert.Equal([]interface{}{`test1`}, values)

	values, ok = f.GetValues(`Enabled`)
	assert.True(ok)
	assert.Equal([]interface{}{true}, values)

	values, ok = f.GetValues(`Age`)
	assert.True(ok)
	assert.Equal([]interface{}{42}, values)
}

func TestFilterParsePtrToStruct(t *testing.T) {
	assert := require.New(t)

	type fCoolObject struct {
		Name    string `pivot:"name"`
		Enabled bool
		Age     int `pivot:",omitempty"`
	}

	f, err := Parse(&fCoolObject{
		Name: `test1`,
	})

	assert.NoError(err)
	assert.Equal(2, len(f.Criteria))

	values, ok := f.GetValues(`name`)
	assert.True(ok)
	assert.Equal([]interface{}{`test1`}, values)

	values, ok = f.GetValues(`Enabled`)
	assert.True(ok)
	assert.Equal([]interface{}{false}, values)

	values, ok = f.GetValues(`Age`)
	assert.False(ok)
	assert.Nil(values)
}
