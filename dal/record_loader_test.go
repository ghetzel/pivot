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

func TestGetIdentityFieldName(t *testing.T) {
	assert := require.New(t)

	f := TestRecord{
		ID: 1234,
	}

	id, err := GetIdentityFieldName(&f, ``)
	assert.Nil(err)
	assert.Equal(`ID`, id)

	f = TestRecord{}
	id, err = GetIdentityFieldName(&f, `Size`)
	assert.Nil(err)
	assert.Equal(`Size`, id)

	f2 := TestRecordTwo{`42`}
	id, err = GetIdentityFieldName(&f2, ``)
	assert.Equal(`UUID`, id)

	f3 := TestRecordThree{}
	id, err = GetIdentityFieldName(&f3, ``)
	assert.Error(err)

	f4 := TestRecordThree{}
	id, err = GetIdentityFieldName(&f4, `UUID`)
	assert.Equal(`UUID`, id)
}
