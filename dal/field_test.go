package dal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFieldValidators(t *testing.T) {
	assert := require.New(t)

	field1 := Field{
		Name: `field1`,
		Validator: func(v interface{}) error {
			if fmt.Sprintf("%v", v) == `test` {
				return nil
			} else {
				return fmt.Errorf("Value is not 'test'")
			}
		},
	}

	assert.Nil(field1.Validate(`test`))
	assert.Error(field1.Validate(`not-test`))
}
