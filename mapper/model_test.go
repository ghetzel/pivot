package mapper

import (
	"testing"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/v3"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/stretchr/testify/require"
)

type testTypeWithStringer int

const (
	TestFirst testTypeWithStringer = iota
	TestSecond
	TestThird
)

func (self testTypeWithStringer) String() string {
	switch self {
	case TestFirst:
		return `first`
	case TestSecond:
		return `second`
	case TestThird:
		return `third`
	default:
		return ``
	}
}

func TestModelCRUD(t *testing.T) {
	log.SetLevel(log.DEBUG)
	assert := require.New(t)

	db, err := pivot.NewDatabase(`redis://`)
	assert.Nil(err)

	type ModelOne struct {
		ID      int
		Name    string               `pivot:"name"`
		Enabled bool                 `pivot:"enabled,omitempty"`
		Type    testTypeWithStringer `pivot:"type"`
		Size    int                  `pivot:"size,omitempty"`
	}

	model1 := NewModel(db, &dal.Collection{
		Name: `model_one`,
		Fields: []dal.Field{
			{
				Name: `name`,
				Type: dal.StringType,
				Formatter: func(value interface{}, op dal.FieldOperation) (interface{}, error) {
					return stringutil.Camelize(value), nil
				},
			}, {
				Name: `enabled`,
				Type: dal.BooleanType,
			}, {
				Name: `size`,
				Type: dal.IntType,
			}, {
				Name:         `type`,
				Type:         dal.IntType,
				DefaultValue: TestFirst,
			},
		},
	})

	assert.Nil(model1.Migrate())

	assert.Nil(model1.Create(&ModelOne{
		ID:      1,
		Name:    `test-1`,
		Enabled: true,
		Size:    12345,
		Type:    TestSecond,
	}))

	v := new(ModelOne)
	err = model1.Get(1, v)

	assert.Nil(err)
	assert.Equal(1, v.ID)
	assert.Equal(`Test1`, v.Name)
	assert.Equal(true, v.Enabled)
	assert.Equal(12345, v.Size)
	assert.Equal(TestSecond, v.Type)

	v.Name = `testerly-one`
	v.Type = TestThird
	assert.Nil(model1.Update(v))

	v = new(ModelOne)
	err = model1.Get(1, v)

	assert.Nil(err)
	assert.Equal(1, v.ID)
	assert.Equal(`TesterlyOne`, v.Name)
	assert.Equal(true, v.Enabled)
	assert.Equal(12345, v.Size)
	assert.Equal(TestThird, v.Type)

	assert.Nil(model1.Delete(1))
	assert.Error(model1.Get(1, nil))
	assert.Nil(model1.Drop())
}

func TestModelFind(t *testing.T) {
	assert := require.New(t)

	db, err := pivot.NewDatabase(`sqlite://`)
	assert.Nil(err)

	type ModelTwo struct {
		ID      int
		Name    string `pivot:"name"`
		Enabled bool   `pivot:"enabled,omitempty"`
		Size    int    `pivot:"size,omitempty"`
	}

	model := NewModel(db, &dal.Collection{
		Name: `model_one`,
		Fields: []dal.Field{
			{
				Name: `name`,
				Type: dal.StringType,
			}, {
				Name: `enabled`,
				Type: dal.BooleanType,
			}, {
				Name: `size`,
				Type: dal.IntType,
			},
		},
	})

	assert.Nil(model.Migrate())

	assert.Nil(model.Create(&ModelTwo{
		ID:      1,
		Name:    `test-one`,
		Enabled: true,
		Size:    12345,
	}))

	assert.Nil(model.Create(&ModelTwo{
		ID:      2,
		Name:    `test-two`,
		Enabled: false,
		Size:    98765,
	}))

	assert.Nil(model.Create(&ModelTwo{
		ID:      3,
		Name:    `test-three`,
		Enabled: true,
	}))

	var resultsStruct []ModelTwo
	assert.Error(model.All(resultsStruct))

	assert.NoError(model.All(&resultsStruct))
	assert.Equal(3, len(resultsStruct))

	var recordset dal.RecordSet

	assert.Error(model.All(recordset))
	assert.NoError(model.All(&recordset))
	assert.Equal(int64(3), recordset.ResultCount)
	assert.Nil(model.Drop())
}

func TestModelList(t *testing.T) {
	assert := require.New(t)
	db, err := pivot.NewDatabase(`sqlite://`)
	assert.Nil(err)

	type ModelTwo struct {
		ID      int
		Name    string `pivot:"name"`
		Enabled bool   `pivot:"enabled,omitempty"`
		Size    int    `pivot:"size,omitempty"`
	}

	model := NewModel(db, &dal.Collection{
		Name: `model_one`,
		Fields: []dal.Field{
			{
				Name: `name`,
				Type: dal.StringType,
			}, {
				Name: `enabled`,
				Type: dal.BooleanType,
			}, {
				Name: `size`,
				Type: dal.IntType,
			},
		},
	})

	assert.Nil(model.Migrate())

	assert.Nil(model.Create(&ModelTwo{
		ID:      1,
		Name:    `test-one`,
		Enabled: true,
		Size:    12345,
	}))

	assert.Nil(model.Create(&ModelTwo{
		ID:      2,
		Name:    `test-two`,
		Enabled: false,
		Size:    98765,
	}))

	assert.Nil(model.Create(&ModelTwo{
		ID:      3,
		Name:    `test-three`,
		Enabled: true,
	}))

	values, err := model.List([]string{`name`})
	assert.Nil(err)
	assert.Equal(map[string][]interface{}{
		`name`: []interface{}{
			`test-one`,
			`test-two`,
			`test-three`,
		},
	}, values)

	values, err = model.List([]string{`name`, `size`})
	assert.Nil(err)
	assert.Equal(map[string][]interface{}{
		`name`: []interface{}{
			`test-one`,
			`test-two`,
			`test-three`,
		},
		`size`: []interface{}{
			int64(12345),
			int64(98765),
		},
	}, values)
}
