package dal

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRecordGet(t *testing.T) {
	assert := require.New(t)

	record := NewRecord(`1`).Set(`this.is.a.test`, true)
	record.SetNested(`this.is.a.test`, `correct`)

	assert.Equal(`1`, record.ID)

	assert.Equal(map[string]interface{}{
		`this.is.a.test`: true,
		`this`: map[string]interface{}{
			`is`: map[string]interface{}{
				`a`: map[string]interface{}{
					`test`: `correct`,
				},
			},
		},
	}, record.Fields)

	assert.Equal(`correct`, record.GetNested(`this.is.a.test`))
	assert.Equal(true, record.Get(`this.is.a.test`))

}

func TestRecordAppend(t *testing.T) {
	assert := require.New(t)

	record := NewRecord(`2`).Append(`first`, 1)
	assert.Equal([]interface{}{1}, record.Get(`first`))

	record = NewRecord(`2a`).Set(`second`, 4)
	assert.Equal(4, record.Get(`second`))
	record.Append(`second`, 5)
	assert.Equal([]interface{}{4, 5}, record.Get(`second`))

	record = NewRecord(`2b`).Set(`third`, []string{`6`, `7`})
	record.Append(`third`, 8)
	assert.Equal([]interface{}{`6`, `7`, 8}, record.Get(`third`))

	record = NewRecord(`2c`).Set(`fourth`, []interface{}{`6`, `7`})
	record.Append(`fourth`, 8)
	record.Append(`fourth`, 9)
	record.Append(`fourth`, `10`)
	assert.Equal([]interface{}{`6`, `7`, 8, 9, `10`}, record.Get(`fourth`))
}

func TestRecordAppendNested(t *testing.T) {
	assert := require.New(t)

	record := NewRecord(`2`).AppendNested(`t.first`, 1)
	assert.Equal([]interface{}{1}, record.Get(`t.first`))

	record = NewRecord(`2a`).SetNested(`t.second`, 4)
	assert.Equal(4, record.Get(`t.second`))
	record.Append(`t.second`, 5)
	assert.Equal([]interface{}{4, 5}, record.Get(`t.second`))

	record = NewRecord(`2b`).SetNested(`t.third`, []string{`6`, `7`})
	record.Append(`t.third`, 8)
	assert.Equal([]interface{}{`6`, `7`, 8}, record.Get(`t.third`))

	record = NewRecord(`2c`).SetNested(`t.fourth`, []interface{}{`6`, `7`})
	record.AppendNested(`t.fourth`, 8)
	record.AppendNested(`t.fourth`, 9)
	record.AppendNested(`t.fourth`, `10`)
	assert.Equal([]interface{}{`6`, `7`, 8, 9, `10`}, record.Get(`t.fourth`))
}
