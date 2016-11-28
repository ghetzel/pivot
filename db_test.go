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
	if b, err := makeBackend(`bolt:///./test.db`); err == nil {
		backend = b
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		os.Exit(1)
	}

	i := m.Run()
	os.Remove(`./test.db`)
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
		Name: `test-collmgmt`,
	})

	assert.Nil(err)

	if coll, err := backend.GetCollection(`test-collmgmt`); err == nil {
		assert.Equal(`test-collmgmt`, coll.Name)
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
	assert.Nil(backend.InsertRecords(`test-crud`, dal.NewRecordSet(
		dal.NewRecord(`1`).Set(`name`, `First`),
		dal.NewRecord(`2`).Set(`name`, `Second`).SetData(TestData),
		dal.NewRecord(`3`).Set(`name`, `Third`))))

	record, err = backend.GetRecordById(`test-crud`, `1`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(dal.Identity(`1`), record.ID)
	assert.Equal(`First`, record.Get(`name`))
	assert.Nil(record.Data)

	record, err = backend.GetRecordById(`test-crud`, `2`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(dal.Identity(`2`), record.ID)
	assert.Equal(`Second`, record.Get(`name`))
	assert.Equal(TestData, record.Data)

	record, err = backend.GetRecordById(`test-crud`, `3`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(dal.Identity(`3`), record.ID)
	assert.Equal(`Third`, record.Get(`name`))
	assert.Nil(record.Data)

	// Update and Retrieve
	// --------------------------------------------------------------------------------------------
	assert.Nil(backend.UpdateRecords(`test-crud`, dal.NewRecordSet(
		dal.NewRecord(`3`).Set(`name`, `Threeve`))))

	record, err = backend.GetRecordById(`test-crud`, `3`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(dal.Identity(`3`), record.ID)
	assert.Equal(`Threeve`, record.Get(`name`))

	// Retrieve-Delete-Verify
	// --------------------------------------------------------------------------------------------
	record, err = backend.GetRecordById(`test-crud`, `2`)
	assert.Nil(err)
	assert.Equal(dal.Identity(`2`), record.ID)

	assert.Nil(backend.DeleteRecords(`test-crud`, []dal.Identity{`2`}))

	record, err = backend.GetRecordById(`test-crud`, `2`)
	assert.NotNil(err)
	assert.Nil(record)
}

func TestSearchQuery(t *testing.T) {
	assert := require.New(t)

	if search := backend.WithSearch(); search != nil {
		err := backend.CreateCollection(dal.Collection{
			Name: `test-search`,
		})

		assert.Nil(err)
		var recordset *dal.RecordSet
		var record *dal.Record
		var ok bool

		assert.Nil(backend.InsertRecords(`test-search`, dal.NewRecordSet(
			dal.NewRecord(`1`).Set(`name`, `First`),
			dal.NewRecord(`2`).Set(`name`, `Second`),
			dal.NewRecord(`3`).Set(`name`, `Third`))))

		for _, qs := range []string{
			`id/1`,
			`id/lt:2`,
			`name/first`,
			`name/First`,
			`name/contains:irs`,
			`name/prefix:fir`,
		} {
			recordset, err = search.QueryString(`test-search`, qs)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.Equal(1, recordset.ResultCount)
			record, ok = recordset.GetRecord(0)
			assert.True(ok)
			assert.NotNil(record)
			assert.Equal(dal.Identity(`1`), record.ID)
			assert.Equal(`First`, record.Get(`name`))
		}
	}
}
