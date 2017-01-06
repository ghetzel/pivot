package filter

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFilterParse(t *testing.T) {
	assert := require.New(t)

	CriteriaSeparator = `/`
	FieldTermSeparator = `/`

	tests := map[string]func(Filter, error){
		`all`: func(f Filter, err error) {
			assert.Nil(err)
			assert.True(f.MatchAll)
			assert.Equal(f.Spec, `all`)
			assert.Equal(0, len(f.Criteria))
		},
		`k1/contains:v1/int:k2/lt:v2a|v2b`: func(f Filter, err error) {
			assert.Nil(err)
			assert.Equal(2, len(f.Criteria))

			assert.Equal(``, f.Criteria[0].Type)
			assert.Equal(`k1`, f.Criteria[0].Field)
			assert.Equal(`contains`, f.Criteria[0].Operator)
			assert.Equal([]string{`v1`}, f.Criteria[0].Values)

			assert.Equal(`int`, f.Criteria[1].Type)
			assert.Equal(`k2`, f.Criteria[1].Field)
			assert.Equal(`lt`, f.Criteria[1].Operator)
			assert.Equal([]string{`v2a`, `v2b`}, f.Criteria[1].Values)
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
	assert.Nil(err)
	assert.Equal(1, len(filter.Criteria))
	assert.Equal(`str`, filter.Criteria[0].Type)
	assert.Equal(16, filter.Criteria[0].Length)
	assert.Equal(`name`, filter.Criteria[0].Field)
	assert.Equal(`prefix`, filter.Criteria[0].Operator)
	assert.Equal([]string{`foo`}, filter.Criteria[0].Values)

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

	tests := map[string]func(Filter, error){
		`all`: func(f Filter, err error) {
			assert.Nil(err)
			assert.True(f.MatchAll)
			assert.Equal(f.Spec, `all`)
			assert.Equal(0, len(f.Criteria))
		},
		`k1=contains:v1 int:k2=lt:v2a|v2b`: func(f Filter, err error) {
			assert.Nil(err)
			assert.Equal(2, len(f.Criteria))

			assert.Equal(``, f.Criteria[0].Type)
			assert.Equal(`k1`, f.Criteria[0].Field)
			assert.Equal(`contains`, f.Criteria[0].Operator)
			assert.Equal([]string{`v1`}, f.Criteria[0].Values)

			assert.Equal(`int`, f.Criteria[1].Type)
			assert.Equal(`k2`, f.Criteria[1].Field)
			assert.Equal(`lt`, f.Criteria[1].Operator)
			assert.Equal([]string{`v2a`, `v2b`}, f.Criteria[1].Values)
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
	})

	assert.Nil(err)
	assert.Equal(3, len(f.Criteria))

	for _, criterion := range f.Criteria {
		switch criterion.Field {
		case `f1`:
			assert.Equal([]string{`v1`}, criterion.Values)

		case `f2`:
			assert.Equal(`int`, criterion.Type)
			assert.Equal([]string{`2`}, criterion.Values)

		case `f3`:
			assert.Equal(`float`, criterion.Type)
			assert.Equal(`gte`, criterion.Operator)
			assert.Equal([]string{`3`}, criterion.Values)
		default:
			t.Errorf("Unknown field %q", criterion.Field)
		}
	}
}

func TestFilterGetSort(t *testing.T) {
	assert := require.New(t)

	f, err := Parse(`name/test/-age/4/+group/one`)
	assert.Nil(err)

	sortBy := f.GetSort()

	assert.Equal(2, len(sortBy))

	assert.Equal(`age`, sortBy[0].Field)
	assert.True(sortBy[0].Descending)

	assert.Equal(`group`, sortBy[1].Field)
	assert.False(sortBy[1].Descending)
}
