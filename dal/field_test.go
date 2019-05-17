package dal

import (
	"fmt"
	"testing"
	"time"

	"github.com/ghetzel/go-stockutil/utils"
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

func TestFieldConvertValueInteger(t *testing.T) {
	assert := require.New(t)
	var field *Field
	var value interface{}
	var err error

	field = &Field{
		Type: IntType,
	}

	// not required, no default
	// -------------------------------------------------------------------------
	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.EqualValues(int64(0), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(int64(0), value)

	value, err = field.ConvertValue(`1234`)
	assert.NoError(err)
	assert.Equal(int64(1234), value)

	// required, no default
	// -------------------------------------------------------------------------
	field.Required = true

	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.Equal(int64(0), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(int64(0), value)

	value, err = field.ConvertValue(`9876`)
	assert.NoError(err)
	assert.Equal(int64(9876), value)

	// required, with default
	// -------------------------------------------------------------------------
	field.DefaultValue = int32(69)

	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.Equal(int64(69), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(int64(69), value)

	value, err = field.ConvertValue(42)
	assert.NoError(err)
	assert.Equal(int64(42), value)
}

func TestFieldConvertValueFloat(t *testing.T) {
	assert := require.New(t)
	var field *Field
	var value interface{}
	var err error

	field = &Field{
		Type: FloatType,
	}

	// not required, no default
	// -------------------------------------------------------------------------
	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.EqualValues(float64(0), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(float64(0), value)

	value, err = field.ConvertValue(`3.141597`)
	assert.NoError(err)
	assert.Equal(float64(3.141597), value)

	// required, no default
	// -------------------------------------------------------------------------
	field.Required = true

	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.Equal(float64(0), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(float64(0), value)

	value, err = field.ConvertValue(`2.71828`)
	assert.NoError(err)
	assert.Equal(float64(2.71828), value)

	// required, with default
	// -------------------------------------------------------------------------
	field.DefaultValue = int32(69)

	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.Equal(float64(69), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(float64(69), value)

	value, err = field.ConvertValue(0.000000000001)
	assert.NoError(err)
	assert.Equal(float64(0.000000000001), value)
}

func TestFieldConvertValueTime(t *testing.T) {
	assert := require.New(t)
	var field *Field
	var value interface{}
	var err error

	field = &Field{
		Type: TimeType,
	}

	// not required, no default
	// -------------------------------------------------------------------------
	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.True(value.(time.Time).IsZero())

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.True(value.(time.Time).IsZero())

	value, err = field.ConvertValue(`2006-01-02T15:04:05-07:00`)
	assert.NoError(err)
	assert.True(utils.ReferenceTime.Equal(value.(time.Time)))

	// required, no default
	// -------------------------------------------------------------------------
	field.Required = true

	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.True(value.(time.Time).IsZero())

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.True(value.(time.Time).IsZero())

	value, err = field.ConvertValue(1136239445)
	assert.NoError(err)
	assert.True(utils.ReferenceTime.Equal(value.(time.Time)))

	// required, with default
	// -------------------------------------------------------------------------
	field.DefaultValue = time.Unix(1136239445, 0)

	value, err = field.ConvertValue(``)
	assert.NoError(err)
	assert.True(utils.ReferenceTime.Equal(value.(time.Time)))

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.True(utils.ReferenceTime.Equal(value.(time.Time)))

	value, err = field.ConvertValue(1500000000)
	assert.NoError(err)
	assert.True(time.Date(2017, 7, 14, 2, 40, 0, 0, time.UTC).Equal(value.(time.Time)))
}

func TestFieldConvertValueObject(t *testing.T) {
	assert := require.New(t)
	var field *Field
	var value interface{}
	var err error

	field = &Field{
		Type: ObjectType,
	}

	// not required, no default
	// -------------------------------------------------------------------------
	value, err = field.ConvertValue(map[string]interface{}{})
	assert.NoError(err)
	assert.EqualValues(make(map[string]interface{}), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Nil(value)

	value, err = field.ConvertValue(map[string]interface{}{
		`value`: 123,
	})
	assert.NoError(err)
	assert.Equal(map[string]interface{}{
		`value`: 123,
	}, value)

	type fieldConvertStruct struct {
		Name  string
		Value int
	}

	value, err = field.ConvertValue(fieldConvertStruct{
		Name:  `test`,
		Value: 123,
	})

	assert.NoError(err)
	assert.Equal(map[string]interface{}{
		`Name`:  `test`,
		`Value`: float64(123),
	}, value)

	// required, no default
	// -------------------------------------------------------------------------
	field.Required = true

	value, err = field.ConvertValue(map[string]interface{}{})
	assert.NoError(err)
	assert.Equal(make(map[string]interface{}), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(make(map[string]interface{}), value)

	value, err = field.ConvertValue(map[string]interface{}{
		`value`: 456,
	})
	assert.NoError(err)
	assert.Equal(map[string]interface{}{
		`value`: 456,
	}, value)

	// required, with default
	// -------------------------------------------------------------------------
	field.DefaultValue = map[string]string{
		`value`: `yay`,
		`other`: `1234.5`,
	}

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(map[string]interface{}{
		`value`: `yay`,
		`other`: `1234.5`,
	}, value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(map[string]interface{}{
		`value`: `yay`,
		`other`: `1234.5`,
	}, value)

	value, err = field.ConvertValue(map[string]interface{}{
		`value`: `ohhh`,
	})
	assert.NoError(err)
	assert.Equal(map[string]interface{}{
		`value`: `ohhh`,
	}, value)
}

func TestFieldConvertValueArray(t *testing.T) {
	assert := require.New(t)
	var field *Field
	var value interface{}
	var err error

	field = &Field{
		Type: ArrayType,
	}

	// not required, no default
	// -------------------------------------------------------------------------
	value, err = field.ConvertValue([]interface{}{})
	assert.NoError(err)
	assert.EqualValues(make([]interface{}, 0), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Nil(value)

	value, err = field.ConvertValue([]interface{}{1, 2, 3})
	assert.NoError(err)
	assert.Equal([]interface{}{1, 2, 3}, value)

	value, err = field.ConvertValue([]string{`1`, `2`, `3`})
	assert.NoError(err)
	assert.Equal([]interface{}{`1`, `2`, `3`}, value)

	// required, no default
	// -------------------------------------------------------------------------
	field.Required = true

	value, err = field.ConvertValue([]interface{}{})
	assert.NoError(err)
	assert.Equal(make([]interface{}, 0), value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal(make([]interface{}, 0), value)

	value, err = field.ConvertValue([]bool{true, true, false})
	assert.NoError(err)
	assert.Equal([]interface{}{true, true, false}, value)

	// required, with default
	// -------------------------------------------------------------------------
	field.DefaultValue = []string{`a`, `b`, `c`}

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal([]interface{}{`a`, `b`, `c`}, value)

	value, err = field.ConvertValue(nil)
	assert.NoError(err)
	assert.Equal([]interface{}{`a`, `b`, `c`}, value)

	value, err = field.ConvertValue([]int64{9, 8, 7})
	assert.NoError(err)
	assert.Equal([]interface{}{int64(9), int64(8), int64(7)}, value)
}
