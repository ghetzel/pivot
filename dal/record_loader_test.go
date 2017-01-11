package dal

import (
    "github.com/stretchr/testify/require"
    "testing"
)

type TestRecord struct {
    Model `pivot:"test_records"`
    ID int
    Name string `pivot:"name,omitempty"`
    Size int `pivot:"size"`
}

type TestRecordTwo struct {
    UUID string `pivot:"uuid,identity"`
}

type TestRecordThree struct {
    UUID string
}

func TestGetCollectionAndIdentity(t *testing.T) {
    assert := require.New(t)

    f := TestRecord{
        ID: 1234,
    }

    c, id, err := GetCollectionAndIdentity(&f)
    assert.Nil(err)
    assert.Equal(`test_records`, c)
    assert.Equal(1234, id)

    f = TestRecord{}
    c, id, err = GetCollectionAndIdentity(&f)
    assert.Nil(err)
    assert.Zero(id)

    f2 := TestRecordTwo{`42`}
    c, id, err = GetCollectionAndIdentity(&f2)
    assert.Nil(err)
    assert.Equal(`TestRecordTwo`, c)
    assert.Equal(`42`, id)

    f3 := TestRecordThree{}
    c, id, err = GetCollectionAndIdentity(&f3)
    assert.Error(err)
}
