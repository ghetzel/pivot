package filter

import (
	"testing"

	"github.com/ghetzel/pivot/v3/dal"
	"github.com/stretchr/testify/require"
)

func TestFilterMatchesRecord(t *testing.T) {
	assert := require.New(t)

	assert.True(MustParse(`id/1`).MatchesRecord(dal.NewRecord(1)))
	assert.True(MustParse(`id/1`).MatchesRecord(dal.NewRecord(`1`)))
	assert.True(MustParse(`id/is:1`).MatchesRecord(dal.NewRecord(1)))
	assert.True(MustParse(`id/is:1`).MatchesRecord(dal.NewRecord(`1`)))
	assert.True(MustParse(`int:id/1`).MatchesRecord(dal.NewRecord(1)))
	assert.True(MustParse(`str:id/1`).MatchesRecord(dal.NewRecord(`1`)))
	assert.False(MustParse(`str:id/is:1`).MatchesRecord(dal.NewRecord(1)))
	assert.True(MustParse(`id/not:1`).MatchesRecord(dal.NewRecord(2)))
	assert.True(MustParse(`id/not:1`).MatchesRecord(dal.NewRecord(`2`)))

	assert.True(MustParse(`id/1/test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, true)))
	assert.True(MustParse(`id/1/test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, `true`)))
	assert.True(MustParse(`id/1/bool:test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, true)))
	assert.True(MustParse(`id/1/str:test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, `true`)))

	assert.False(MustParse(`id/1/test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, false)))
	assert.False(MustParse(`id/1/test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, `false`)))
	assert.False(MustParse(`id/1/test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, 1)))
	assert.False(MustParse(`id/1/test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, `1`)))
	assert.False(MustParse(`id/1/test/false`).MatchesRecord(dal.NewRecord(1).Set(`test`, 0)))
	assert.False(MustParse(`id/1/test/false`).MatchesRecord(dal.NewRecord(1).Set(`test`, `0`)))
	assert.False(MustParse(`id/1/str:test/true`).MatchesRecord(dal.NewRecord(1).Set(`test`, true)))

	assert.False(MustParse(`id/gt:1`).MatchesRecord(dal.NewRecord(0)))
	assert.False(MustParse(`id/gt:1`).MatchesRecord(dal.NewRecord(1)))
	assert.True(MustParse(`id/gt:1`).MatchesRecord(dal.NewRecord(2)))
	assert.False(MustParse(`id/gte:1`).MatchesRecord(dal.NewRecord(0)))
	assert.True(MustParse(`id/gte:1`).MatchesRecord(dal.NewRecord(1)))
	assert.True(MustParse(`id/gte:1`).MatchesRecord(dal.NewRecord(2)))

	assert.False(MustParse(`id/lt:1`).MatchesRecord(dal.NewRecord(2)))
	assert.False(MustParse(`id/lt:1`).MatchesRecord(dal.NewRecord(1)))
	assert.True(MustParse(`id/lt:1`).MatchesRecord(dal.NewRecord(0)))
	assert.False(MustParse(`id/lte:1`).MatchesRecord(dal.NewRecord(2)))
	assert.True(MustParse(`id/lte:1`).MatchesRecord(dal.NewRecord(1)))
	assert.True(MustParse(`id/lte:1`).MatchesRecord(dal.NewRecord(0)))

	assert.True(MustParse(`name/contains:old`).MatchesRecord(dal.NewRecord(1).Set(`name`, `Goldenrod`)))
	assert.True(MustParse(`name/prefix:gold`).MatchesRecord(dal.NewRecord(1).Set(`name`, `Gold`)))
	assert.True(MustParse(`name/prefix:Gold`).MatchesRecord(dal.NewRecord(1).Set(`name`, `Gold`)))
	assert.True(MustParse(`name/suffix:rod`).MatchesRecord(dal.NewRecord(1).Set(`name`, `Goldenrod`)))

	assert.True(MustParse(`name/contains:olden rod`).MatchesRecord(dal.NewRecord(1).Set(`name`, `Golden rod`)))
	assert.True(MustParse(`name/Golden rod`).MatchesRecord(dal.NewRecord(1).Set(`name`, `Golden rod`)))
	assert.True(MustParse(`name/like:golden rod`).MatchesRecord(dal.NewRecord(1).Set(`name`, `Golden rod`)))
}
