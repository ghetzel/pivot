package backends

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisSplitKey(t *testing.T) {
	assert := require.New(t)

	collection, keys := redisSplitKey(``)
	assert.Zero(collection)
	assert.Empty(keys)

	collection, keys = redisSplitKey(`testing:123`)
	assert.Equal(`testing`, collection)
	assert.Equal([]string{`123`}, keys)

	collection, keys = redisSplitKey(`pivot.testing:123`)
	assert.Equal(`testing`, collection)
	assert.Equal([]string{`123`}, keys)

	collection, keys = redisSplitKey(`pivot.testing:123:456`)
	assert.Equal(`testing`, collection)
	assert.Equal([]string{`123`, `456`}, keys)

	collection, keys = redisSplitKey(`pivot.deeply.nested.whatsawhat.testing:123:456`)
	assert.Equal(`testing`, collection)
	assert.Equal([]string{`123`, `456`}, keys)
}
