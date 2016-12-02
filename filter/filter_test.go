package filter

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFilterParse(t *testing.T) {
	assert := require.New(t)

	tests := map[string]func(Filter, error){
		`all`: func(f Filter, err error) {
			assert.Nil(err)
			assert.True(f.MatchAll)
			assert.Equal(f.Spec, `all`)
			assert.Equal(0, len(f.Criteria))
		},
	}

	for spec, fn := range tests {
		f, err := Parse(spec)
		fn(f, err)
	}
}
