package pivot

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var backend backends.Backend
var TestData = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

func setupTestSqlite() {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)

	if b, err := makeBackend(`sqlite:///./tmp/db_test/test.db`); err == nil {
		backend = b
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		os.Exit(1)
	}
}

func setupTestMysql() {
	if b, err := makeBackend(`mysql://test:test@db/test`); err == nil {
		backend = b
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		os.Exit(1)
	}
}

func setupTestBoltDB() {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)
	backends.BleveBatchFlushCount = 1

	if b, err := makeBackend(`boltdb:///./tmp/db_test`); err == nil {
		backend = b
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		os.Exit(1)
	}
}

func TestMain(m *testing.M) {
	var i int

	run := func() {
		i = m.Run()

		if i != 0 {
			os.Exit(i)
		}
	}

	setupTestMysql()
	run()

	setupTestSqlite()
	run()

	setupTestBoltDB()
	run()
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

	defer func() {
		assert.Nil(backend.DeleteCollection(`TestCollectionManagement`))
	}()

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
		Name: `TestBasicCRUD`,
		Fields: []dal.Field{
			{
				Name: `name`,
				Type: `str`,
			},
		},
	})

	defer func() {
		assert.Nil(backend.DeleteCollection(`TestBasicCRUD`))
	}()

	assert.Nil(err)
	var record *dal.Record

	// Insert and Retrieve
	// --------------------------------------------------------------------------------------------
	assert.Nil(backend.Insert(`TestBasicCRUD`, dal.NewRecordSet(
		dal.NewRecord(`1`).Set(`name`, `First`),
		dal.NewRecord(`2`).Set(`name`, `Second`),
		dal.NewRecord(`3`).Set(`name`, `Third`))))

	record, err = backend.Retrieve(`TestBasicCRUD`, `1`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(string(`1`), record.ID)
	assert.Equal(`First`, record.Get(`name`))
	assert.Empty(record.Data)

	record, err = backend.Retrieve(`TestBasicCRUD`, `2`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(string(`2`), record.ID)
	assert.Equal(`Second`, record.Get(`name`))

	record, err = backend.Retrieve(`TestBasicCRUD`, `3`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(string(`3`), record.ID)
	assert.Equal(`Third`, record.Get(`name`))

	// make sure we can json encode the record, too
	_, err = json.Marshal(record)
	assert.Nil(err)

	// Update and Retrieve
	// --------------------------------------------------------------------------------------------
	assert.Nil(backend.Update(`TestBasicCRUD`, dal.NewRecordSet(
		dal.NewRecord(`3`).Set(`name`, `Threeve`))))

	record, err = backend.Retrieve(`TestBasicCRUD`, `3`)
	assert.Nil(err)
	assert.NotNil(record)
	assert.Equal(string(`3`), record.ID)
	assert.Equal(`Threeve`, record.Get(`name`))

	// Retrieve-Delete-Verify
	// --------------------------------------------------------------------------------------------
	record, err = backend.Retrieve(`TestBasicCRUD`, `2`)
	assert.Nil(err)
	assert.Equal(string(`2`), record.ID)

	f, err := filter.Parse(fmt.Sprintf("id/2"))
	assert.Nil(err)
	assert.Nil(backend.Delete(`TestBasicCRUD`, f))

	record, err = backend.Retrieve(`TestBasicCRUD`, `2`)
	assert.NotNil(err)
	assert.Nil(record)
}

func TestSearchQuery(t *testing.T) {
	assert := require.New(t)

	if search := backend.WithSearch(); search != nil {
		err := backend.CreateCollection(dal.Collection{
			Name: `TestSearchQuery`,
			Fields: []dal.Field{
				{
					Name: `name`,
					Type: `str`,
				},
			},
		})

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestSearchQuery`))
		}()

		assert.Nil(err)
		var recordset *dal.RecordSet
		var record *dal.Record
		var ok bool

		assert.Nil(backend.Insert(`TestSearchQuery`, dal.NewRecordSet(
			dal.NewRecord(`1`).Set(`name`, `First`),
			dal.NewRecord(`2`).Set(`name`, `Second`),
			dal.NewRecord(`3`).Set(`name`, `Third`))))

		// twosies
		for _, qs := range []string{
			`name/contains:ir`,
			`name/suffix:d`,
		} {
			t.Logf("Querying (want 2 results): %q\n", qs)
			f, err := filter.Parse(qs)
			assert.Nil(err)
			recordset, err = search.Query(`TestSearchQuery`, f)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.Equal(int64(2), recordset.ResultCount)
		}

		// onesies
		for _, qs := range []string{
			`id/1`,
			`name/First`,
			`name/first`,
			`name/contains:irs`,
			`name/contains:irS`,
			`name/prefix:fir`,
			`name/prefix:fIr`,
			`name/contains:ir/name/prefix:f`,
			`name/contains:ir/name/prefix:F`,
		} {
			t.Logf("Querying (want 1 result): %q\n", qs)
			f, err := filter.Parse(qs)
			assert.Nil(err)
			recordset, err = search.Query(`TestSearchQuery`, f)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.Equal(int64(1), recordset.ResultCount)
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
			f, err := filter.Parse(qs)
			assert.Nil(err)
			recordset, err = search.Query(`TestSearchQuery`, f)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.Equal(int64(0), recordset.ResultCount)
			assert.True(recordset.IsEmpty())
		}
	}
}

func TestSearchQueryPaginated(t *testing.T) {
	assert := require.New(t)

	// set the global page size at the package level for this test
	backends.IndexerPageSize = 5

	if search := backend.WithSearch(); search != nil {
		err := backend.CreateCollection(dal.Collection{
			Name: `TestSearchQueryPaginated`,
		})

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestSearchQueryPaginated`))
		}()

		assert.Nil(err)

		rsSave := dal.NewRecordSet()

		for i := 0; i < 21; i++ {
			rsSave.Push(dal.NewRecord(fmt.Sprintf("%d", i)))
		}

		assert.Nil(backend.Insert(`TestSearchQueryPaginated`, rsSave))

		f := filter.All
		f.Limit = 25

		recordset, err := search.Query(`TestSearchQueryPaginated`, f)
		assert.Nil(err)

		assert.NotNil(recordset)
		assert.Equal(21, len(recordset.Records))

		if !recordset.Unbounded {
			assert.Equal(int64(21), recordset.ResultCount)
			assert.Equal(1, recordset.TotalPages)
		}
	}
}

func TestSearchQueryLimit(t *testing.T) {
	assert := require.New(t)
	backends.IndexerPageSize = 100

	if search := backend.WithSearch(); search != nil {
		err := backend.CreateCollection(dal.Collection{
			Name:              `TestSearchQueryLimit`,
			IdentityFieldType: `str`,
		})

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestSearchQueryLimit`))
		}()

		assert.Nil(err)

		rsSave := dal.NewRecordSet()

		for i := 0; i < 21; i++ {
			rsSave.Push(dal.NewRecord(fmt.Sprintf("%02d", i)))
		}

		assert.Nil(backend.Insert(`TestSearchQueryLimit`, rsSave))

		f, err := filter.Parse(`all`)
		assert.Nil(err)

		f.Limit = 9

		recordset, err := search.Query(`TestSearchQueryLimit`, f)
		assert.Nil(err)
		assert.NotNil(recordset)

		assert.Equal(9, len(recordset.Records))

		if !recordset.Unbounded {
			assert.Equal(int64(21), recordset.ResultCount)
			assert.Equal(3, recordset.TotalPages)
		}

		record, ok := recordset.GetRecord(0)
		assert.True(ok)
		assert.NotNil(record)
		assert.Equal(`00`, record.ID)
	}
}

func TestSearchQueryOffset(t *testing.T) {
	assert := require.New(t)
	backends.IndexerPageSize = 100

	if search := backend.WithSearch(); search != nil {
		err := backend.CreateCollection(dal.Collection{
			Name:              `TestSearchQueryOffset`,
			IdentityFieldType: `str`,
		})

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestSearchQueryOffset`))
		}()

		assert.Nil(err)

		rsSave := dal.NewRecordSet()

		for i := 0; i < 21; i++ {
			rsSave.Push(dal.NewRecord(fmt.Sprintf("%02d", i)))
		}

		assert.Nil(backend.Insert(`TestSearchQueryOffset`, rsSave))

		f, err := filter.Parse(`all`)
		assert.Nil(err)

		f.Offset = 20

		recordset, err := search.Query(`TestSearchQueryOffset`, f)
		assert.Nil(err)
		assert.NotNil(recordset)
		assert.Equal(1, len(recordset.Records))

		if !recordset.Unbounded {
			assert.Equal(int64(21), recordset.ResultCount)
			assert.Equal(1, recordset.TotalPages)
		}

		record, ok := recordset.GetRecord(0)
		assert.True(ok)
		assert.NotNil(record)
		assert.Equal(`20`, record.ID)
	}
}

func TestSearchQueryOffsetLimit(t *testing.T) {
	assert := require.New(t)

	if search := backend.WithSearch(); search != nil {
		old := backends.IndexerPageSize
		backends.IndexerPageSize = 3

		defer func() {
			backends.IndexerPageSize = old
		}()

		err := backend.CreateCollection(dal.Collection{
			Name:              `TestSearchQueryOffsetLimit`,
			IdentityFieldType: `str`,
		})

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestSearchQueryOffsetLimit`))
		}()

		assert.Nil(err)

		rsSave := dal.NewRecordSet()

		for i := 0; i < 21; i++ {
			rsSave.Push(dal.NewRecord(fmt.Sprintf("%02d", i)))
		}

		assert.Nil(backend.Insert(`TestSearchQueryOffsetLimit`, rsSave))

		f, err := filter.Parse(`all`)
		assert.Nil(err)

		f.Offset = 3
		f.Limit = 9

		recordset, err := search.Query(`TestSearchQueryOffsetLimit`, f)
		assert.Nil(err)
		assert.NotNil(recordset)
		assert.Equal(9, len(recordset.Records))

		if !recordset.Unbounded {
			assert.Equal(int64(21), recordset.ResultCount)
			assert.Equal(3, recordset.TotalPages)
		}

		record, ok := recordset.GetRecord(0)
		assert.True(ok)
		assert.NotNil(record)
		assert.Equal(`03`, record.ID)
	}
}

func TestListValues(t *testing.T) {
	assert := require.New(t)

	if search := backend.WithSearch(); search != nil {
		err := backend.CreateCollection(dal.Collection{
			Name: `TestListValues`,
			Fields: []dal.Field{
				{
					Name: `name`,
					Type: `str`,
				}, {
					Name: `group`,
					Type: `str`,
				},
			},
		})

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestListValues`))
		}()

		assert.Nil(err)

		assert.Nil(backend.Insert(`TestListValues`, dal.NewRecordSet(
			dal.NewRecord(`1`).SetFields(map[string]interface{}{
				`name`:  `first`,
				`group`: `reds`,
			}),
			dal.NewRecord(`2`).SetFields(map[string]interface{}{
				`name`:  `second`,
				`group`: `reds`,
			}),
			dal.NewRecord(`3`).SetFields(map[string]interface{}{
				`name`:  `third`,
				`group`: `blues`,
			}))))

		recordset, err := search.ListValues(`TestListValues`, []string{`name`}, filter.All)
		assert.Nil(err)
		assert.NotNil(recordset)
		assert.Equal(int64(1), recordset.ResultCount)
		assert.Equal([]interface{}{`first`, `second`, `third`}, recordset.Records[0].Get(`values`))

		recordset, err = search.ListValues(`TestListValues`, []string{`group`}, filter.All)
		assert.Nil(err)
		assert.NotNil(recordset)
		assert.Equal(int64(1), recordset.ResultCount)
		assert.Equal([]interface{}{`reds`, `blues`}, recordset.Records[0].Get(`values`))

		recordset, err = search.ListValues(`TestListValues`, []string{`id`}, filter.All)
		assert.Nil(err)
		assert.NotNil(recordset)
		assert.Equal(int64(1), recordset.ResultCount)
		assert.Equal([]interface{}{`1`, `2`, `3`}, recordset.Records[0].Get(`values`))

		recordset, err = search.ListValues(`TestListValues`, []string{`id`, `group`}, filter.All)
		assert.Nil(err)
		assert.NotNil(recordset)
		assert.Equal(int64(2), recordset.ResultCount)
		assert.Equal([]interface{}{`1`, `2`, `3`}, recordset.Records[0].Get(`values`))
		assert.Equal([]interface{}{`reds`, `blues`}, recordset.Records[1].Get(`values`))
	}
}

func TestSearchAnalysis(t *testing.T) {
	assert := require.New(t)

	if search := backend.WithSearch(); search != nil {
		err := backend.CreateCollection(dal.Collection{
			Name: `TestSearchAnalysis`,
			Fields: []dal.Field{
				{
					Name: `single`,
					Type: `str`,
				}, {
					Name: `char_filter_test`,
					Type: `str`,
				},
			},
		})

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestSearchAnalysis`))
		}()

		assert.Nil(err)

		assert.Nil(backend.Insert(`TestSearchAnalysis`, dal.NewRecordSet(
			dal.NewRecord(`1`).SetFields(map[string]interface{}{
				`single`:           `first-result`,
				`char_filter_test`: `this:resUlt`,
			}),
			dal.NewRecord(`2`).SetFields(map[string]interface{}{
				`single`:           `second-result`,
				`char_filter_test`: `This[Result`,
			}),
			dal.NewRecord(`3`).SetFields(map[string]interface{}{
				`single`:           `third-result`,
				`char_filter_test`: `this*result`,
			}))))

		// threesies
		for _, qs := range []string{
			`single/contains:result`,
			`single/suffix:result`,
			`char_filter_test/this result`,
		} {
			t.Logf("Querying (want 3 results): %q\n", qs)
			f, err := filter.Parse(qs)
			assert.Nil(err)
			recordset, err := search.Query(`TestSearchAnalysis`, f)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.Equal(int64(3), recordset.ResultCount)
		}
	}
}
