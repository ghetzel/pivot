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
