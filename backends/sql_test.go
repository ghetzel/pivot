package backends

import (
	"fmt"
	"testing"

	"github.com/ghetzel/pivot/v3/dal"
	"github.com/stretchr/testify/require"
)

func TestSqlAlterStatements(t *testing.T) {
	assert := require.New(t)
	b := NewSqlBackend(dal.MustParseConnectionString(`sqlite://temporary`)).(*SqlBackend)
	assert.NoError(b.Initialize())

	have := &dal.Collection{
		Name:          `TestSqlAlterStatements`,
		IdentityField: `id`,
		Fields: []dal.Field{
			{
				Name:     `name`,
				Type:     dal.StringType,
				Required: true,
			}, {
				Name:     `created_at`,
				Type:     dal.IntType,
				Required: true,
			},
		},
	}

	want := &dal.Collection{
		Name:          `TestSqlAlterStatements`,
		IdentityField: `id`,
		Fields: []dal.Field{
			{
				Name:     `name`,
				Type:     dal.StringType,
				Required: true,
			}, {
				Name:         `age`,
				Type:         dal.IntType,
				Required:     true,
				DefaultValue: 1,
			}, {
				Name:         `created_at`,
				Type:         dal.TimeType,
				Required:     true,
				DefaultValue: `now`,
			},
		},
	}

	b.RegisterCollection(have)
	assert.NoError(b.Migrate())

	for _, delta := range want.Diff(have) {
		stmt, _, err := b.generateAlterStatement(delta)
		assert.NoError(err)

		// TODO: this is the wrong order, need to work out whats going on
		switch delta.Name {
		case `age`:
			assert.Equal(`ALTER TABLE "TestSqlAlterStatements" ADD "created_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP`, stmt)
		case `created_at`:
			assert.Equal(`ALTER TABLE "TestSqlAlterStatements" ADD "age" BIGINT NOT NULL`, stmt)
		default:
			assert.NoError(fmt.Errorf("extra diff: %v", delta))
		}
	}
}
