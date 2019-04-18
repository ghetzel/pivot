package dal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type TestRecord struct {
	Model `pivot:"test_records"`
	ID    int
	Name  string `pivot:"name,omitempty"`
	Size  int    `pivot:"size"`
}

type TestRecordTwo struct {
	UUID string `pivot:"uuid,identity"`
}

type TestRecordThree struct {
	UUID string
}

func TestGetIdentityFieldNameFromStruct(t *testing.T) {
	assert := require.New(t)

	f := TestRecord{
		ID: 1234,
	}

	field, key, err := getIdentityFieldNameFromStruct(&f, ``)
	assert.Nil(err)
	assert.Equal(`ID`, key)
	assert.Equal(`ID`, key)

	f = TestRecord{}
	field, key, err = getIdentityFieldNameFromStruct(&f, `Size`)
	assert.Nil(err)
	assert.Equal(`Size`, field)
	assert.Equal(`Size`, key)

	f2 := TestRecordTwo{`42`}
	field, key, err = getIdentityFieldNameFromStruct(&f2, ``)
	assert.Equal(`UUID`, field)
	assert.Equal(`uuid`, key)

	f3 := TestRecordThree{}
	field, key, err = getIdentityFieldNameFromStruct(&f3, ``)
	assert.Error(err)

	f4 := TestRecordThree{}
	field, key, err = getIdentityFieldNameFromStruct(&f4, `UUID`)
	assert.Equal(`UUID`, field)
	assert.Equal(`UUID`, key)
}
