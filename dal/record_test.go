package dal

import (
	"fmt"
	"testing"

	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/stretchr/testify/require"
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

func TestRecordPopulateStruct(t *testing.T) {
	assert := require.New(t)

	type testThing struct {
		ID    int
		Name  string `pivot:"name"`
		Group string `pivot:"Group,omitempty"`
		Size  int
	}

	thing := testThing{}
	record := NewRecord(1).Set(`name`, `test-name`).Set(`Size`, 42)

	err := record.Populate(&thing, nil)
	assert.Nil(err)
	assert.Equal(`test-name`, thing.Name)
	assert.Zero(thing.Group)
	assert.Equal(42, thing.Size)

	thing = testThing{
		Group: `tests`,
	}

	record = NewRecord(1).Set(`name`, `test-name`).Set(`Size`, 42)

	err = record.Populate(&thing, nil)
	assert.Nil(err)
	assert.Equal(`test-name`, thing.Name)
	assert.Equal(`tests`, thing.Group)
	assert.Equal(42, thing.Size)

	type KV struct {
		Key   string      `pivot:"key,identity"`
		Value interface{} `pivot:"value,omitempty"`
	}

	record = NewRecord(`this.is.an.id`).Set(`value`, 42)
	kv := new(KV)

	err = record.Populate(kv, nil)
	assert.Nil(err)
	assert.Equal(`this.is.an.id`, kv.Key)
	assert.Equal(42, kv.Value)
}

func TestRecordPopulateStructWithValidator(t *testing.T) {
	assert := require.New(t)

	collection := &Collection{
		Name: `TestRecordPopulateStructWithValidator`,
		Fields: []Field{
			{
				Name: `name`,
				Type: StringType,
				Validator: func(v interface{}) error {
					if fmt.Sprintf("%v", v) == `test` {
						return fmt.Errorf("Shouldn't be test")
					}

					return nil
				},
				ValidateOnPopulate: true,
			},
		},
	}

	type testThing struct {
		ID    int
		Name  string `pivot:"name"`
		Group string `pivot:"Group,omitempty"`
		Size  int
	}

	thing := testThing{}
	record := NewRecord(1).Set(`name`, `test`).Set(`Size`, 42)

	assert.Error(record.Populate(&thing, collection))

	// make sure populate will succeed if we're not validating on populate
	collection.Fields[0].ValidateOnPopulate = false
	assert.Nil(record.Populate(&thing, collection))
	collection.Fields[0].ValidateOnPopulate = true

	thing = testThing{
		Name:  `Booberry`,
		Group: `tests`,
	}

	record = NewRecord(1).Set(`name`, `test-name`).Set(`Size`, 42)

	err := record.Populate(&thing, collection)
	assert.Nil(err)
	assert.Equal(`test-name`, thing.Name)

	// this remains untouched because this field isn't in the collection
	assert.Equal(`tests`, thing.Group)
	assert.Zero(thing.Size)
}

func TestRecordPopulateStructWithFormatter(t *testing.T) {
	assert := require.New(t)

	collection := &Collection{
		Name: `TestRecordPopulateStructWithFormatter`,
		Fields: []Field{
			{
				Name: `name`,
				Type: StringType,
				Formatter: func(v interface{}, op FieldOperation) (interface{}, error) {
					return stringutil.Underscore(fmt.Sprintf("%v", v)), nil
				},
			},
		},
	}

	type testThing struct {
		ID    int
		Name  string `pivot:"name"`
		Group string `pivot:"Group,omitempty"`
		Size  int
	}

	thing := testThing{}
	record := NewRecord(1).Set(`name`, `TestName`).Set(`Size`, 42)

	err := record.Populate(&thing, collection)
	assert.Nil(err)
	assert.Equal(`test_name`, thing.Name)
	assert.Zero(thing.Group)
	assert.Zero(thing.Size)

	thing = testThing{
		Name:  `Booberry`,
		Group: `tests`,
	}

	record = NewRecord(1).Set(`name`, `test-name`).Set(`Size`, 42)

	err = record.Populate(&thing, collection)
	assert.Nil(err)
	assert.Equal(`test_name`, thing.Name)

	// this remains untouched because this field isn't in the collection
	assert.Equal(`tests`, thing.Group)
	assert.Zero(thing.Size)
}

func TestRecordPopulateStructWithFormatterValidator(t *testing.T) {
	assert := require.New(t)

	collection := &Collection{
		Name: `TestRecordPopulateStructWithFormatterValidator`,
		Fields: []Field{
			{
				Name: `name`,
				Type: StringType,
				Validator: func(v interface{}) error {
					if fmt.Sprintf("%v", v) == `TestValue` {
						return fmt.Errorf("Shouldn't be TestValue")
					}

					return nil
				},
				Formatter: func(v interface{}, op FieldOperation) (interface{}, error) {
					return stringutil.Underscore(fmt.Sprintf("%v", v)), nil
				},
			},
		},
	}

	type testThing struct {
		ID    int
		Name  string `pivot:"name"`
		Group string `pivot:"Group,omitempty"`
		Size  int
	}

	thing := testThing{}
	record := NewRecord(1).Set(`name`, `TestValue`).Set(`Size`, 42)

	err := record.Populate(&thing, collection)
	assert.Nil(err)
	assert.Equal(`test_value`, thing.Name)
}
