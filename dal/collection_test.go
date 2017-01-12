package dal

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollectionMakeRecord(t *testing.T) {
	assert := require.New(t)

	collection := NewCollection(`TestCollectionMakeRecord`)
	collection.AddFields([]Field{
		{
			Name: `name`,
			Type: StringType,
		}, {
			Name: `enabled`,
			Type: BooleanType,
		}, {
			Name: `age`,
			Type: IntType,
		},
	}...)

	type TestRecord struct {
		Name      string `pivot:"name"`
		Enabled   bool   `pivot:"enabled,omitempty"`
		ActualAge int    `pivot:"age"`
		Age       int
	}

	testRecord := TestRecord{
		Name:      `tester`,
		ActualAge: 42,
	}

	record, err := collection.MakeRecord(&testRecord)
	assert.Nil(err)
	assert.NotNil(record)

	assert.Equal(2, len(record.Fields))

	assert.Nil(record.ID)
	assert.Equal(`tester`, record.Get(`name`))
	assert.Equal(42, record.Get(`age`))

	type TestRecord2 struct {
		ID        int
		Name      string `pivot:"name"`
		Enabled   bool   `pivot:"enabled,omitempty"`
		ActualAge int    `pivot:"age"`
		Age       int
	}

	testRecord2 := TestRecord2{
		ID:   11,
		Name: `tester`,
	}

	record, err = collection.MakeRecord(&testRecord2)
	assert.Nil(err)
	assert.NotNil(record)

	assert.Equal(2, len(record.Fields))

	assert.Equal(11, record.ID)
	assert.Equal(`tester`, record.Get(`name`))
	assert.Equal(0, record.Get(`age`))
}
