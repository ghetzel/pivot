package dal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type TestRecord struct {
	ID   int
	Name string `pivot:"name,omitempty"`
	Size int    `pivot:"size"`
}

type TestRecordTwo struct {
	UUID string `pivot:"uuid,identity"`
}

type TestRecordThree struct {
	UUID string
}

type TestRecordEmbedded struct {
	TestRecordTwo
	TestRecord
	Local bool
}

func TestGetIdentityFieldNameFromStruct(t *testing.T) {
	assert := require.New(t)

	f := TestRecord{
		ID: 1234,
	}

	field, key, err := getIdentityFieldNameFromStruct(&f, ``)
	assert.NoError(err)
	assert.Equal(`ID`, key)
	assert.Equal(`ID`, key)

	f = TestRecord{}
	field, key, err = getIdentityFieldNameFromStruct(&f, `Size`)
	assert.NoError(err)
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

	f5 := TestRecordEmbedded{}
	f5.UUID = `42`
	f5.UUID = `42`
	f5.ID = 42
	f5.Name = `Fourty Two`
	f5.Size = 42
	f5.Local = true

	field, key, err = getIdentityFieldNameFromStruct(&f5, ``)
	assert.NoError(err)
	assert.Equal(`UUID`, field)
	assert.Equal(`uuid`, key)
}
