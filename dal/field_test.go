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

func TestFieldConvertValueString(t *testing.T) {
	assert := require.New(t)
	var field *Field
	var value interface{}
	var err error

	field = &Field{
		Type: StringType,
	}

	// not required, no default
	// -------------------------------------------------------------------------
	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.Equal(``, value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(``, value)

	value, err = field.ConvertValue(`things`)
	assert.NoError(err)
	assert.Equal(`things`, value)

	// required, no default
	// -------------------------------------------------------------------------
	field.Required = true

	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.Equal(``, value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(``, value)

	value, err = field.ConvertValue(`things`)
	assert.NoError(err)
	assert.Equal(`things`, value)

	// required, with default
	// -------------------------------------------------------------------------
	field.DefaultValue = `test`

	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.Equal(`test`, value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(`test`, value)

	value, err = field.ConvertValue(`things`)
	assert.NoError(err)
	assert.Equal(`things`, value)
}

func TestFieldConvertValueBool(t *testing.T) {
	assert := require.New(t)
	var field *Field
	var value interface{}
	var err error

	field = &Field{
		Type: BooleanType,
	}

	// not required, no default
	// -------------------------------------------------------------------------
	for _, v := range []interface{}{
		true,
		`true`,
		`True`,
		`TRUE`,
		`yes`,
		`on`,
		`YES`,
		`ON`,
		1,
		-4,
		3.14,
		42,
		`dennis`,
	} {
		value, err = field.ConvertValue(v)
		assert.NoError(err)
		assert.Equal(true, value, fmt.Sprintf("output: %T(%v) -> %T(%v)", v, v, value, value))
	}

	for _, v := range []interface{}{
		false,
		`false`,
		`False`,
		`FALSE`,
		`no`,
		`off`,
		`NO`,
		`OFF`,
		0,
	} {
		value, err = field.ConvertValue(v)
		assert.NoError(err)
		assert.Equal(false, value, fmt.Sprintf("output: %T(%v) -> %T(%v)", v, v, value, value))
	}
	// required, no default
	// -------------------------------------------------------------------------
	field.Required = true

	for _, v := range []interface{}{
		true,
		`true`,
		`True`,
		`TRUE`,
		`yes`,
		`on`,
		`YES`,
		`ON`,
		1,
		-4,
		3.14,
		42,
		`dennis`,
	} {
		value, err = field.ConvertValue(v)
		assert.NoError(err)
		assert.Equal(true, value, fmt.Sprintf("output: %T(%v) -> %T(%v)", v, v, value, value))
	}

	for _, v := range []interface{}{
		false,
		`false`,
		`False`,
		`FALSE`,
		`no`,
		`off`,
		`NO`,
		`OFF`,
		nil,
		0,
	} {
		value, err = field.ConvertValue(v)
		assert.NoError(err)
		assert.Equal(false, value, fmt.Sprintf("output: %T(%v) -> %T(%v)", v, v, value, value))
	}

	// required, with default
	// -------------------------------------------------------------------------
	field.DefaultValue = true

	for _, v := range []interface{}{
		true,
		`true`,
		`True`,
		`TRUE`,
		`yes`,
		`on`,
		`YES`,
		`ON`,
		1,
		-4,
		3.14,
		42,
		`dennis`,
	} {
		value, err = field.ConvertValue(v)
		assert.NoError(err)
		assert.Equal(true, value, fmt.Sprintf("output: %T(%v) -> %T(%v)", v, v, value, value))
	}

	for _, v := range []interface{}{
		false,
		`false`,
		`False`,
		`FALSE`,
		`no`,
		`off`,
		`NO`,
		`OFF`,
		nil,
		0,
	} {
		value, err = field.ConvertValue(v)
		assert.NoError(err)
		assert.Equal(true, value, fmt.Sprintf("output: %T(%v) -> %T(%v)", v, v, value, value))
	}

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(true, value, fmt.Sprintf("output: nil -> %T(%v)", value, value))
}

// TODO: basically the *worst* thing you can write in a file full of tests
// func TestFieldConvertValueInteger(t *testing.T) {}
// func TestFieldConvertValueFloat(t *testing.T) {}
// func TestFieldConvertValueTime(t *testing.T) {}
// func TestFieldConvertValueObject(t *testing.T) {}
// func TestFieldConvertValueRaw(t *testing.T) {}
