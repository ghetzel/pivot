package pivot

import (
	"fmt"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var backend backends.Backend
var TestData = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

func TestMain(m *testing.M) {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)

	if b, err := makeBackend(`boltdb:///./tmp/db_test`); err == nil {
		backend = b
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		os.Exit(1)
	}

	i := m.Run()
	os.Exit(i)
}

func makeBackend(conn string) (backends.Backend, error) {
	if cs, err := dal.ParseConnectionString(conn); err == nil {
		if backend, err := backends.MakeBackend(cs); err == nil {
			if err := backend.Initialize(); err == nil {
				return backend, nil
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func TestCollectionManagement(t *testing.T) {
	assert := require.New(t)

	err := backend.CreateCollection(dal.Collection{
		Name: `TestCollectionManagement`,
	})

	assert.Nil(err)

	if coll, err := backend.GetCollection(`TestCollectionManagement`); err == nil {
		assert.Equal(`TestCollectionManagement`, coll.Name)
	} else {
		assert.Nil(err)
	}
}

func TestBasicCRUD(t *testing.T) {
	assert := require.New(t)

	err := backend.CreateCollection(dal.Collection{
		Name: `test-crud`,
	})

	assert.Nil(err)
	var record *dal.Record

	// Insert and Retrieve
	// --------------------------------------------------------------------------------------------
	assert.Nil(backend.InsertRecords(`TestBasicCRUD`, dal.NewRecordSet(
		dal.NewRecord(`1`).Set(`name`, `First`),
		dal.NewRecord(`2`).Set(`name`, `Second`).SetData(TestData),
		dal.NewRecord(`3`).Set(`name`, `Third`))))

	record, err = backend.GetRecordById(`TestBasicCRUD`, `1`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(string(`1`), record.ID)
	assert.Equal(`First`, record.Get(`name`))
	assert.Nil(record.Data)

	record, err = backend.GetRecordById(`TestBasicCRUD`, `2`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(string(`2`), record.ID)
	assert.Equal(`Second`, record.Get(`name`))
	assert.Equal(TestData, record.Data)

	record, err = backend.GetRecordById(`TestBasicCRUD`, `3`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(string(`3`), record.ID)
	assert.Equal(`Third`, record.Get(`name`))
	assert.Nil(record.Data)

	// Update and Retrieve
	// --------------------------------------------------------------------------------------------
	assert.Nil(backend.UpdateRecords(`TestBasicCRUD`, dal.NewRecordSet(
		dal.NewRecord(`3`).Set(`name`, `Threeve`))))

	record, err = backend.GetRecordById(`TestBasicCRUD`, `3`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(string(`3`), record.ID)
	assert.Equal(`Threeve`, record.Get(`name`))

	// Retrieve-Delete-Verify
	// --------------------------------------------------------------------------------------------
	record, err = backend.GetRecordById(`TestBasicCRUD`, `2`)
	assert.Nil(err)
	assert.Equal(string(`2`), record.ID)

	assert.Nil(backend.DeleteRecords(`TestBasicCRUD`, []string{`2`}))

	record, err = backend.GetRecordById(`TestBasicCRUD`, `2`)
	assert.NotNil(err)
	assert.Nil(record)
}

func TestSearchQuery(t *testing.T) {
	assert := require.New(t)

	if search := backend.WithSearch(); search != nil {
		err := backend.CreateCollection(dal.Collection{
			Name: `TestSearchQuery`,
		})

		assert.Nil(err)
		var recordset *dal.RecordSet
		var record *dal.Record
		var ok bool

		assert.Nil(backend.InsertRecords(`TestSearchQuery`, dal.NewRecordSet(
			dal.NewRecord(`1`).Set(`name`, `First`),
			dal.NewRecord(`2`).Set(`name`, `Second`),
			dal.NewRecord(`3`).Set(`name`, `Third`))))

		// twosies
		for _, qs := range []string{
			`name/contains:ir`,
			`name/suffix:d`,
		} {
			t.Logf("Querying (want 2 results): %q\n", qs)
			recordset, err = search.QueryString(`TestSearchQuery`, qs)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.Equal(uint64(2), recordset.ResultCount)
		}

		// onesies
		for _, qs := range []string{
			`_id/1`,
			`name/first`,
			`name/First`,
			`name/contains:irs`,
			`name/prefix:fir`,
			`name/contains:ir/name/prefix:f`,
		} {
			t.Logf("Querying (want 1 result): %q\n", qs)
			recordset, err = search.QueryString(`TestSearchQuery`, qs)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.Equal(uint64(1), recordset.ResultCount)
			record, ok = recordset.GetRecord(0)
			assert.True(ok)
			assert.NotNil(record)
			assert.Equal(string(`1`), record.ID)
			assert.Equal(`First`, record.Get(`name`))
		}

		// nonesies
		for _, qs := range []string{
			`name/contains:irs/name/prefix:sec`,
		} {
			t.Logf("Querying (want 0 results): %q\n", qs)
			recordset, err = search.QueryString(`TestSearchQuery`, qs)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.Equal(uint64(0), recordset.ResultCount)
			assert.True(recordset.IsEmpty())
		}
	}
}
