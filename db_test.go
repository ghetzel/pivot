package pivot

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/stretchr/testify/require"
)

var backend backends.Backend
var TestData = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

var defaultTestCrudIdSet = []interface{}{`1`, `2`, `3`}
var testCrudIdSet = []interface{}{`1`, `2`, `3`}

func setupTestSqlite(run func()) {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)

	if b, err := makeBackend(`sqlite:///./tmp/db_test/test.db`); err == nil {
		backend = b
		run()
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestSqliteWithBleveIndexer(run func()) {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)

	if b, err := makeBackend(`sqlite:///./tmp/db_test/test.db`, backends.ConnectOptions{
		Indexer: `bleve:///./tmp/db_test/`,
	}); err == nil {
		backend = b
		run()
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestSqliteWithAdditionalBleveIndexer(run func()) {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)

	if b, err := makeBackend(`sqlite:///./tmp/db_test/test.db`, backends.ConnectOptions{
		AdditionalIndexers: []string{
			`bleve:///./tmp/db_test/`,
		},
	}); err == nil {
		backend = b
		run()
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestMysql(run func()) {
	if b, err := makeBackend(`mysql://test:test@db/test`); err == nil {
		backend = b
		run()
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestPostgres(run func()) {
	if b, err := makeBackend(`postgres://test:test@db/test`); err == nil {
		backend = b
		run()
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestFilesystemDefault(run func()) {
	if root, err := ioutil.TempDir(``, `pivot-backend-fs-default-`); err == nil {
		defer os.RemoveAll(root)

		if b, err := makeBackend(fmt.Sprintf("fs://%s/", root)); err == nil {
			backend = b
			run()
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		}
	} else {
		panic(err.Error())
	}
}

func setupTestFilesystemYaml(run func()) {
	if root, err := ioutil.TempDir(``, `pivot-backend-fs-yaml-`); err == nil {
		defer os.RemoveAll(root)

		if b, err := makeBackend(fmt.Sprintf("fs+yaml://%s/", root)); err == nil {
			backend = b
			run()
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		}
	} else {
		panic(err.Error())
	}
}

func setupTestFilesystemJson(run func()) {
	if root, err := ioutil.TempDir(``, `pivot-backend-fs-json-`); err == nil {
		defer os.RemoveAll(root)

		if b, err := makeBackend(fmt.Sprintf("fs+json://%s/", root)); err == nil {
			backend = b
			run()
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		}
	} else {
		panic(err.Error())
	}
}

func setupTestTiedot(run func()) {
	if root, err := ioutil.TempDir(``, `pivot-backend-tiedot-`); err == nil {
		defer os.RemoveAll(root)

		if b, err := makeBackend(fmt.Sprintf("tiedot://%s/", root)); err == nil {
			backend = b

			testCrudIdSet = []interface{}{nil, nil, nil}
			run()
			testCrudIdSet = defaultTestCrudIdSet
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		}
	} else {
		panic(err.Error())
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

	// setupTestMysql(run)
	// setupTestTiedot(run)
	// setupTestSqlite(run)
	// setupTestSqliteWithBleveIndexer(run)
	// setupTestSqliteWithAdditionalBleveIndexer(run)
	// setupTestFilesystemDefault(run)
	// setupTestFilesystemYaml(run)
	setupTestFilesystemJson(run)
}

func makeBackend(conn string, options ...backends.ConnectOptions) (backends.Backend, error) {
	if cs, err := dal.ParseConnectionString(conn); err == nil {
		if backend, err := backends.MakeBackend(cs); err == nil {
			if len(options) > 0 {
				backend.SetOptions(options[0])
			}

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

	err := backend.CreateCollection(dal.NewCollection(`TestCollectionManagement`))

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

	err := backend.CreateCollection(
		dal.NewCollection(`TestBasicCRUD`).
			AddFields(dal.Field{
				Name: `name`,
				Type: dal.StringType,
			}, dal.Field{
				Name:         `created_at`,
				Type:         dal.TimeType,
				DefaultValue: time.Now,
			}))

	defer func() {
		assert.Nil(backend.DeleteCollection(`TestBasicCRUD`))
	}()

	assert.Nil(err)
	var record *dal.Record

	// Insert and Retrieve
	// --------------------------------------------------------------------------------------------
	recordset := dal.NewRecordSet(
		dal.NewRecord(testCrudIdSet[0]).Set(`name`, `First`),
		dal.NewRecord(testCrudIdSet[1]).Set(`name`, `Second`),
		dal.NewRecord(testCrudIdSet[2]).Set(`name`, `Third`))

	assert.Nil(backend.Insert(`TestBasicCRUD`, recordset))

	assert.True(backend.Exists(`TestBasicCRUD`, fmt.Sprintf("%v", recordset.Records[0].ID)))
	assert.True(backend.Exists(`TestBasicCRUD`, recordset.Records[0].ID))
	assert.False(backend.Exists(`TestBasicCRUD`, `99`))
	assert.False(backend.Exists(`TestBasicCRUD`, 99))

	record, err = backend.Retrieve(`TestBasicCRUD`, fmt.Sprintf("%v", recordset.Records[0].ID))
	assert.Nil(err)
	assert.NotNil(record)

	if testCrudIdSet[0] == nil {
		assert.Equal(recordset.Records[0].ID.(int64), record.ID)
	} else {
		assert.Equal(int64(1), record.ID)
	}

	assert.Equal(`First`, record.Get(`name`))
	assert.Empty(record.Data)
	v := record.Get(`created_at`)
	assert.NotNil(v)
	vTime, ok := v.(time.Time)
	assert.True(ok)
	assert.False(vTime.IsZero())

	record, err = backend.Retrieve(`TestBasicCRUD`, fmt.Sprintf("%v", recordset.Records[1].ID))
	assert.Nil(err)
	assert.NotNil(record)

	if testCrudIdSet[1] == nil {
		assert.Equal(recordset.Records[1].ID.(int64), record.ID)
	} else {
		assert.Equal(int64(2), record.ID)
	}

	assert.Equal(`Second`, record.Get(`name`))

	record, err = backend.Retrieve(`TestBasicCRUD`, fmt.Sprintf("%v", recordset.Records[2].ID))
	assert.Nil(err)
	assert.NotNil(record)

	if testCrudIdSet[2] == nil {
		assert.Equal(recordset.Records[2].ID.(int64), record.ID)
	} else {
		assert.Equal(int64(3), record.ID)
	}

	assert.Equal(`Third`, record.Get(`name`))

	// make sure we can json encode the record, too
	_, err = json.Marshal(record)
	assert.Nil(err)

	// Update and Retrieve
	// --------------------------------------------------------------------------------------------
	assert.Nil(backend.Update(`TestBasicCRUD`, dal.NewRecordSet(
		dal.NewRecord(fmt.Sprintf("%v", recordset.Records[2].ID)).Set(`name`, `Threeve`))))

	record, err = backend.Retrieve(`TestBasicCRUD`, fmt.Sprintf("%v", recordset.Records[2].ID))
	assert.Nil(err)
	assert.NotNil(record)

	if testCrudIdSet[2] == nil {
		assert.Equal(recordset.Records[2].ID.(int64), record.ID)
	} else {
		assert.Equal(int64(3), record.ID)
	}

	assert.Equal(`Threeve`, record.Get(`name`))

	// Retrieve-Delete-Verify
	// --------------------------------------------------------------------------------------------
	record, err = backend.Retrieve(`TestBasicCRUD`, fmt.Sprintf("%v", recordset.Records[1].ID))
	assert.Nil(err)

	if testCrudIdSet[1] == nil {
		assert.Equal(recordset.Records[1].ID.(int64), record.ID)
	} else {
		assert.Equal(int64(2), record.ID)
	}

	assert.Nil(backend.Delete(`TestBasicCRUD`, recordset.Records[1].ID))
}

func TestSearchQuery(t *testing.T) {
	assert := require.New(t)

	if search := backend.WithSearch(`TestSearchQuery`); search != nil {
		err := backend.CreateCollection(
			dal.NewCollection(`TestSearchQuery`).
				AddFields(dal.Field{
					Name: `name`,
					Type: dal.StringType,
				}))

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
			assert.Equal(int64(1), record.ID)
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

	if search := backend.WithSearch(`TestSearchQueryPaginated`); search != nil {
		err := backend.CreateCollection(dal.NewCollection(`TestSearchQueryPaginated`))

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestSearchQueryPaginated`))
		}()

		assert.Nil(err)

		rsSave := dal.NewRecordSet()

		for i := 0; i < 21; i++ {
			rsSave.Push(
				dal.NewRecord(fmt.Sprintf("%d", i+1)),
			)
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

	if search := backend.WithSearch(`TestSearchQueryLimit`); search != nil {
		c := dal.NewCollection(`TestSearchQueryLimit`)
		c.IdentityFieldType = dal.StringType
		err := backend.CreateCollection(c)

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

	if search := backend.WithSearch(`TestSearchQueryOffset`); search != nil {
		c := dal.NewCollection(`TestSearchQueryOffset`)
		c.IdentityFieldType = dal.StringType
		err := backend.CreateCollection(c)

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

	if search := backend.WithSearch(`TestSearchQueryOffsetLimit`); search != nil {
		old := backends.IndexerPageSize
		backends.IndexerPageSize = 3

		defer func() {
			backends.IndexerPageSize = old
		}()

		c := dal.NewCollection(`TestSearchQueryOffsetLimit`)
		c.IdentityFieldType = dal.StringType
		err := backend.CreateCollection(c)

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

	if search := backend.WithSearch(`TestListValues`); search != nil {
		err := backend.CreateCollection(
			dal.NewCollection(`TestListValues`).
				AddFields(dal.Field{
					Name: `name`,
					Type: dal.StringType,
				}, dal.Field{
					Name: `group`,
					Type: dal.StringType,
				}))

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

		keyValues, err := search.ListValues(`TestListValues`, []string{`name`}, filter.All)
		assert.Nil(err)
		assert.Equal(1, len(keyValues))
		v, ok := keyValues[`name`]
		assert.True(ok)
		assert.Equal([]interface{}{`first`, `second`, `third`}, v)

		keyValues, err = search.ListValues(`TestListValues`, []string{`group`}, filter.All)
		assert.Nil(err)
		assert.Equal(1, len(keyValues))
		v, ok = keyValues[`group`]
		assert.True(ok)
		assert.Equal([]interface{}{`reds`, `blues`}, v)

		keyValues, err = search.ListValues(`TestListValues`, []string{`id`}, filter.All)
		assert.Nil(err)
		assert.Equal(1, len(keyValues))
		v, ok = keyValues[`id`]
		assert.True(ok)
		assert.Equal([]interface{}{int64(1), int64(2), int64(3)}, v)

		keyValues, err = search.ListValues(`TestListValues`, []string{`id`, `group`}, filter.All)
		assert.Nil(err)
		assert.Equal(2, len(keyValues))

		v, ok = keyValues[`id`]
		assert.True(ok)
		assert.Equal([]interface{}{int64(1), int64(2), int64(3)}, v)

		v, ok = keyValues[`group`]
		assert.True(ok)
		assert.Equal([]interface{}{`reds`, `blues`}, v)
	}
}

func TestSearchAnalysis(t *testing.T) {
	assert := require.New(t)

	if search := backend.WithSearch(`TestSearchAnalysis`); search != nil {
		err := backend.CreateCollection(
			dal.NewCollection(`TestSearchAnalysis`).
				AddFields(dal.Field{
					Name: `single`,
					Type: dal.StringType,
				}, dal.Field{
					Name: `char_filter_test`,
					Type: dal.StringType,
				}))

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

func TestObjectType(t *testing.T) {
	assert := require.New(t)

	err := backend.CreateCollection(
		dal.NewCollection(`TestObjectType`).
			AddFields(dal.Field{
				Name: `properties`,
				Type: `object`,
			}))

	defer func() {
		assert.Nil(backend.DeleteCollection(`TestObjectType`))
	}()

	assert.Nil(err)
	var record *dal.Record

	// Insert and Retrieve
	// --------------------------------------------------------------------------------------------
	recordset := dal.NewRecordSet(
		dal.NewRecord(testCrudIdSet[0]).Set(`properties`, map[string]interface{}{
			`name`:  `First`,
			`count`: 1,
		}),
		dal.NewRecord(testCrudIdSet[1]).Set(`properties`, map[string]interface{}{
			`name`:    `Second`,
			`count`:   0,
			`enabled`: false,
		}),
		dal.NewRecord(testCrudIdSet[2]).Set(`properties`, map[string]interface{}{
			`name`:  `Third`,
			`count`: 3,
		}))

	assert.Nil(backend.Insert(`TestObjectType`, recordset))

	record, err = backend.Retrieve(`TestObjectType`, recordset.Records[0].ID)
	assert.NoError(err)
	assert.NotNil(record)

	if testCrudIdSet[0] == nil {
		assert.Equal(recordset.Records[0].ID.(int64), record.ID)
	} else {
		assert.Equal(int64(1), record.ID)
	}

	assert.Equal(`First`, record.GetNested(`properties.name`))
	assert.Equal(float64(1), record.GetNested(`properties.count`))
}

func TestAggregators(t *testing.T) {
	assert := require.New(t)

	err := backend.CreateCollection(
		dal.NewCollection(`TestAggregators`).
			AddFields(dal.Field{
				Name: `color`,
				Type: dal.StringType,
			}, dal.Field{
				Name:     `inventory`,
				Type:     dal.IntType,
				Required: true,
			}, dal.Field{
				Name:     `factor`,
				Type:     dal.FloatType,
				Required: true,
			}, dal.Field{
				Name: `created_at`,
				Type: dal.TimeType,
			}))

	defer func() {
		assert.NoError(backend.DeleteCollection(`TestAggregators`))
	}()

	assert.NoError(err)

	if agg := backend.WithAggregator(`TestAggregators`); agg != nil {
		// Insert and Retrieve
		// --------------------------------------------------------------------------------------------
		assert.NoError(backend.Insert(`TestAggregators`, dal.NewRecordSet(
			dal.NewRecord(nil).Set(`color`, `red`).Set(`inventory`, 34).Set(`factor`, float64(2.7)).Set(`created_at`, time.Now()),
			dal.NewRecord(nil).Set(`color`, `green`).Set(`inventory`, 92).Set(`factor`, float64(9.8)).Set(`created_at`, time.Now()),
			dal.NewRecord(nil).Set(`color`, `blue`).Set(`inventory`, 0).Set(`factor`, float64(5.6)).Set(`created_at`, time.Now()),
			dal.NewRecord(nil).Set(`color`, `orange`).Set(`inventory`, 54).Set(`factor`, float64(0)).Set(`created_at`, time.Now()),
			dal.NewRecord(nil).Set(`color`, `yellow`).Set(`inventory`, 123).Set(`factor`, float64(3.14)).Set(`created_at`, time.Now()),
			dal.NewRecord(nil).Set(`color`, `gold`).Set(`inventory`, 19).Set(`factor`, float64(4.67)).Set(`created_at`, time.Now()),
		)))

		vui, err := agg.Count(`TestAggregators`, filter.All)
		assert.NoError(err)
		assert.Equal(uint64(6), vui)

		vf, err := agg.Sum(`TestAggregators`, `inventory`, filter.All)
		assert.NoError(err)
		assert.Equal(float64(322), vf)

		vf, err = agg.Minimum(`TestAggregators`, `inventory`, filter.All)
		assert.NoError(err)
		assert.Equal(float64(0), vf)

		vf, err = agg.Minimum(`TestAggregators`, `factor`, filter.All)
		assert.NoError(err)
		assert.Equal(float64(0), vf)

		vf, err = agg.Maximum(`TestAggregators`, `inventory`, filter.All)
		assert.NoError(err)
		assert.Equal(float64(123), vf)

		vf, err = agg.Maximum(`TestAggregators`, `factor`, filter.All)
		assert.NoError(err)
		assert.Equal(float64(9.8), vf)
	}
}
