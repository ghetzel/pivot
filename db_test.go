package pivot

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghetzel/pivot/v3/mapper"
	"github.com/ory/dockertest"
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

type testRunnerFunc func(backends.Backend)

var backend backends.Backend
var TestData = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

var defaultTestCrudIdSet = []interface{}{`1`, `2`, `3`}
var testCrudIdSet = []interface{}{`1`, `2`, `3`}

func errpanic(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func TestAll(t *testing.T) {
	log.SetLevel(log.WARNING)

	run := func(b backends.Backend) {
		t.Logf("[%v] Testing CollectionManagement", b)
		testCollectionManagement(t, b)

		t.Logf("[%v] Testing BasicCRUD", b)
		testBasicCRUD(t, b)

		t.Logf("[%v] Testing Load Schemata from File(s)", b)
		testLoadSchema(t, b)

		t.Logf("[%v] Testing Load Fixtures from File(s)", b)
		testLoadFixtures(t, b)

		t.Logf("[%v] Testing IdFormattersRandomId", b)
		testIdFormattersRandomId(t, b)

		t.Logf("[%v] Testing IdFormattersIdFromFieldValues", b)
		testIdFormattersIdFromFieldValues(t, b)

		if b.Supports(backends.CompositeKeys) {
			t.Logf("[%v] Testing CompositeKeyQueries", b)
			testCompositeKeyQueries(t, b)
		}

		t.Logf("[%v] Testing SearchQuery", b)
		testSearchQuery(t, b)

		t.Logf("[%v] Testing SearchQueryPaginated", b)
		testSearchQueryPaginated(t, b)

		t.Logf("[%v] Testing SearchQueryLimit", b)
		testSearchQueryLimit(t, b)

		t.Logf("[%v] Testing SearchQueryOffset", b)
		testSearchQueryOffset(t, b)

		t.Logf("[%v] Testing SearchQueryOffsetLimit", b)
		testSearchQueryOffsetLimit(t, b)

		t.Logf("[%v] Testing ListValues", b)
		testListValues(t, b)

		t.Logf("[%v] Testing SearchAnalysis", b)
		testSearchAnalysis(t, b)

		t.Logf("[%v] Testing ObjectType", b)
		testObjectType(t, b)

		t.Logf("[%v] Testing Aggregators", b)
		testAggregators(t, b)

		t.Logf("[%v] Testing Model CRUD", b)
		testModelCRUD(t, b)

		t.Logf("[%v] Testing Model Find", b)
		testModelFind(t, b)

		t.Logf("[%v] Testing Model List", b)
		testModelList(t, b)
	}

	if typeutil.V(os.Getenv(`CI`)).Bool() {
		runnables := sliceutil.CompactString(strings.Split(os.Getenv(`PIVOT_TEST_BACKENDS`), `,`))

		shouldRun := func(wg *sync.WaitGroup, name string, do func()) {
			wg.Add(1)
			defer wg.Done()

			if len(runnables) == 0 || sliceutil.ContainsString(runnables, name) {
				do()
			} else {
				fmt.Printf("Skipping database suite %q\n", name)
			}
		}

		var waiter sync.WaitGroup

		// go shouldRun(&waiter, `dynamodb`, func() { setupTestDynamoDB(run) })
		// go shouldRun(&waiter, `redis`, func() { setupTestRedis(run) })
		go shouldRun(&waiter, `mongo`, func() { setupTestMongo(`3.2`, run) })
		go shouldRun(&waiter, `mongo`, func() { setupTestMongo(`3.4`, run) })
		go shouldRun(&waiter, `mongo`, func() { setupTestMongo(`3.6`, run) })
		go shouldRun(&waiter, `mongo`, func() { setupTestMongo(`4.0`, run) })
		go shouldRun(&waiter, `psql`, func() { setupTestPostgres(`9`, run) })
		go shouldRun(&waiter, `psql`, func() { setupTestPostgres(`10`, run) })
		go shouldRun(&waiter, `mysql`, func() { setupTestMysql(`5`, run) })
		// go shouldRun(&waiter, `mysql`, func() { setupTestMysql(`8`, run) })
		go shouldRun(&waiter, `sqlite`, func() { setupTestSqlite(run) })
		go shouldRun(&waiter, `sqlite`, func() { setupTestSqliteWithAdditionalBleveIndexer(run) })
		go shouldRun(&waiter, `sqlite`, func() { setupTestSqliteWithBleveIndexer(run) })

		go shouldRun(&waiter, `fs`, func() { setupTestFilesystemJson(run) })
		go shouldRun(&waiter, `fs`, func() { setupTestFilesystemYaml(run) })
		shouldRun(&waiter, `fs`, func() { setupTestFilesystemDefault(run) })

		waiter.Wait()
	} else {
		t.Logf("CI tests not running")
	}
}

func docker(container string, tag string, env map[string]interface{}, pingFn func(res *dockertest.Resource) (backends.Backend, error), runFn testRunnerFunc) {
	pool, err := dockertest.NewPool("")
	errpanic(err)

	envjoin := make([]string, 0)

	for k, v := range env {
		envjoin = append(envjoin, fmt.Sprintf("%v=%v", k, v))
	}

	// pulls an image, creates a container based on it and runs it
	fmt.Printf("Starting container %v:%v\n", container, tag)
	resource, err := pool.Run(container, tag, envjoin)
	errpanic(resource.Expire(120))

	defer pool.Purge(resource)

	var backend backends.Backend

	errpanic(err)
	errpanic(pool.Retry(func() error {
		if b, err := pingFn(resource); err == nil {
			backend = b
			return nil
		} else {
			return err
		}
	}))

	if backend != nil {
		fmt.Printf("Ready\n")
		runFn(backend)
	} else {
		panic(fmt.Sprintf("%v: no backend", container))
	}
}

func setupTestSqlite(run testRunnerFunc) {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)

	if b, err := makeBackend(`sqlite://tmp/db_test/test.db`); err == nil {
		run(b)
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestRedis(run testRunnerFunc) {
	if b, err := makeBackend(`redis://localhost:6379/testing?autoregister=true`); err == nil {
		run(b)
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestSqliteWithBleveIndexer(run testRunnerFunc) {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)

	if b, err := makeBackend(`sqlite://tmp/db_test/test.db`, backends.ConnectOptions{
		Indexer: `bleve:///./tmp/db_test/`,
	}); err == nil {
		run(b)
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestSqliteWithAdditionalBleveIndexer(run testRunnerFunc) {
	os.RemoveAll(`./tmp/db_test`)
	os.MkdirAll(`./tmp/db_test`, 0755)

	if b, err := makeBackend(`sqlite://tmp/db_test/test.db`, backends.ConnectOptions{
		AdditionalIndexers: []string{
			`bleve:///./tmp/db_test/`,
		},
	}); err == nil {
		run(b)
	} else {
		fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
	}
}

func setupTestMysql(version string, run testRunnerFunc) {
	docker(`mysql`, version, map[string]interface{}{
		`MYSQL_ROOT_PASSWORD`: `pivot`,
		`MYSQL_DATABASE`:      `pivot`,
	}, func(res *dockertest.Resource) (backends.Backend, error) {
		if b, err := makeBackend(
			fmt.Sprintf("mysql://root:pivot@localhost:%v/pivot", res.GetPort("3306/tcp")),
		); err == nil {
			return b, b.Ping(time.Second)
		} else {
			return nil, err
		}
	}, run)
}

func setupTestPostgres(version string, run testRunnerFunc) {
	docker(`postgres`, version, map[string]interface{}{
		`POSTGRES_PASSWORD`: `pivot`,
		`POSTGRES_USER`:     `pivot`,
	}, func(res *dockertest.Resource) (backends.Backend, error) {
		if b, err := makeBackend(
			fmt.Sprintf("postgres://pivot:pivot@localhost:%v/pivot", res.GetPort("5432/tcp")),
		); err == nil {
			return b, b.Ping(time.Second)
		} else {
			return nil, err
		}
	}, run)
}

func setupTestFilesystemDefault(run testRunnerFunc) {
	if root, err := ioutil.TempDir(``, `pivot-backend-fs-default-`); err == nil {
		defer os.RemoveAll(root)

		if b, err := makeBackend(fmt.Sprintf("fs://%s/", root)); err == nil {
			run(b)
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		}
	} else {
		panic(err.Error())
	}
}

func setupTestFilesystemYaml(run testRunnerFunc) {
	if root, err := ioutil.TempDir(``, `pivot-backend-fs-yaml-`); err == nil {
		defer os.RemoveAll(root)

		if b, err := makeBackend(fmt.Sprintf("fs+yaml://%s/", root)); err == nil {
			run(b)
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		}
	} else {
		panic(err.Error())
	}
}

func setupTestFilesystemJson(run testRunnerFunc) {
	if root, err := ioutil.TempDir(``, `pivot-backend-fs-json-`); err == nil {
		defer os.RemoveAll(root)

		if b, err := makeBackend(fmt.Sprintf("fs+json://%s/", root)); err == nil {
			run(b)
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create backend: %v\n", err)
		}
	} else {
		panic(err.Error())
	}
}

func setupTestDynamoDB(run testRunnerFunc) {
	if b, err := makeBackend(fmt.Sprintf(
		"dynamodb://%s:%s@%s",
		os.Getenv(`AWS_ACCESS_KEY_ID`),
		os.Getenv(`AWS_SECRET_ACCESS_KEY`),
		`us-east-1`,
	)); err == nil {
		testCrudIdSet = []interface{}{nil, nil, nil}
		run(b)
		testCrudIdSet = defaultTestCrudIdSet
	} else {
		panic(fmt.Sprintf("Failed to create backend: %v\n", err))
	}
}

func setupTestMongo(version string, run testRunnerFunc) {
	docker(`mongo`, version, map[string]interface{}{
		`MONGO_INITDB_ROOT_USERNAME`: `pivot`,
		`MONGO_INITDB_ROOT_PASSWORD`: `pivot`,
		`MONGO_INITDB_DATABASE`:      `pivot`,
	}, func(res *dockertest.Resource) (backends.Backend, error) {
		if b, err := makeBackend(
			fmt.Sprintf("mongodb://pivot:pivot@localhost:%v/pivot?authdb=admin", res.GetPort("27017/tcp")),
		); err == nil {
			return b, b.Ping(time.Second)
		} else {
			return nil, err
		}
	}, run)
}

func makeBackend(conn string, options ...backends.ConnectOptions) (backends.Backend, error) {
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

func testCollectionManagement(t *testing.T, backend backends.Backend) {
	assert := require.New(t)

	err := backend.CreateCollection(dal.NewCollection(`TestCollectionManagement`))

	defer func() {
		assert.NoError(backend.DeleteCollection(`TestCollectionManagement`))
	}()

	assert.NoError(err)

	if coll, err := backend.GetCollection(`TestCollectionManagement`); err == nil {
		assert.Equal(`TestCollectionManagement`, coll.Name)
	} else {
		assert.NoError(err)
	}
}

func testBasicCRUD(t *testing.T, backend backends.Backend) {
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
	assert.IsType(time.Now(), v, fmt.Sprintf("expected time.Time, got %T (value=%v)", v, v))
	assert.False(typeutil.IsZero(v))

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

func testIdFormattersRandomId(t *testing.T, backend backends.Backend) {
	assert := require.New(t)

	assert.Nil(backend.CreateCollection(
		dal.NewCollection(`TestIdFormattersRandomId`).
			SetIdentity(``, dal.StringType, dal.GenerateUUID, nil).
			AddFields(dal.Field{
				Name: `name`,
				Type: dal.StringType,
			}, dal.Field{
				Name:         `created_at`,
				Type:         dal.TimeType,
				DefaultValue: time.Now,
			})))

	defer func() {
		assert.Nil(backend.DeleteCollection(`TestIdFormattersRandomId`))
	}()

	// Insert and Retrieve (UUID)
	// --------------------------------------------------------------------------------------------
	recordset := dal.NewRecordSet(
		dal.NewRecord(nil).Set(`name`, `First`),
		dal.NewRecord(nil).Set(`name`, `Second`),
		dal.NewRecord(nil).Set(`name`, `Third`))

	assert.Equal(3, len(recordset.Records))
	assert.Nil(backend.Insert(`TestIdFormattersRandomId`, recordset))

	assert.NotNil(stringutil.MustUUID(fmt.Sprintf("%v", recordset.Records[0].ID)))
	assert.NotNil(stringutil.MustUUID(fmt.Sprintf("%v", recordset.Records[1].ID)))
	assert.NotNil(stringutil.MustUUID(fmt.Sprintf("%v", recordset.Records[2].ID)))

	record, err := backend.Retrieve(`TestIdFormattersRandomId`, recordset.Records[0].ID)
	assert.NoError(err)
	assert.EqualValues(recordset.Records[0].ID, record.ID)
	assert.Equal(`First`, record.Get(`name`))

	record, err = backend.Retrieve(`TestIdFormattersRandomId`, recordset.Records[1].ID)
	assert.NoError(err)
	assert.EqualValues(recordset.Records[1].ID, record.ID)
	assert.Equal(`Second`, record.Get(`name`))

	record, err = backend.Retrieve(`TestIdFormattersRandomId`, recordset.Records[2].ID)
	assert.NoError(err)
	assert.EqualValues(recordset.Records[2].ID, record.ID)
	assert.Equal(`Third`, record.Get(`name`))
}

func testIdFormattersIdFromFieldValues(t *testing.T, backend backends.Backend) {
	assert := require.New(t)

	assert.Nil(backend.CreateCollection(
		dal.NewCollection(`TestIdFormattersIdFromFieldValues`).
			SetIdentity(``, dal.StringType, dal.DeriveFromFields("%v-%v", `group`, `name`), nil).
			AddFields(dal.Field{
				Name:         `group`,
				Type:         dal.StringType,
				Required:     true,
				DefaultValue: `system`,
			}, dal.Field{
				Name:     `name`,
				Type:     dal.StringType,
				Required: true,
			}, dal.Field{
				Name:         `created_at`,
				Type:         dal.TimeType,
				DefaultValue: time.Now,
			})))

	defer func() {
		assert.Nil(backend.DeleteCollection(`TestIdFormattersIdFromFieldValues`))
	}()

	// Insert and Retrieve (UUID)
	// --------------------------------------------------------------------------------------------
	recordset := dal.NewRecordSet(
		dal.NewRecord(nil).Set(`name`, `first`),
		dal.NewRecord(nil).Set(`name`, `first`).Set(`group`, `users`),
		dal.NewRecord(nil).Set(`name`, `third`))

	assert.Equal(3, len(recordset.Records))
	assert.Nil(backend.Insert(`TestIdFormattersIdFromFieldValues`, recordset))

	assert.Equal(`system-first`, fmt.Sprintf("%v", recordset.Records[0].ID), "%#+v", recordset.Records[0])
	assert.Equal(`users-first`, fmt.Sprintf("%v", recordset.Records[1].ID), "%#+v", recordset.Records[1])
	assert.Equal(`system-third`, fmt.Sprintf("%v", recordset.Records[2].ID), "%#+v", recordset.Records[2])

	record, err := backend.Retrieve(`TestIdFormattersIdFromFieldValues`, recordset.Records[0].ID)
	assert.NoError(err)
	assert.EqualValues(`system-first`, record.ID, "%#+v", record)
	assert.Equal(`first`, record.Get(`name`))

	record, err = backend.Retrieve(`TestIdFormattersIdFromFieldValues`, recordset.Records[1].ID)
	assert.NoError(err)
	assert.EqualValues(`users-first`, record.ID)
	assert.Equal(`first`, record.Get(`name`))
	assert.Equal(`users`, record.Get(`group`))

	record, err = backend.Retrieve(`TestIdFormattersIdFromFieldValues`, recordset.Records[2].ID)
	assert.NoError(err)
	assert.EqualValues(`system-third`, record.ID)
	assert.Equal(`third`, record.Get(`name`))
}

func testSearchQuery(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	collection := dal.NewCollection(`TestSearchQuery`).
		AddFields(dal.Field{
			Name: `name`,
			Type: dal.StringType,
		})

	if search := backend.WithSearch(collection); search != nil {
		err := backend.CreateCollection(collection)

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
			recordset, err = search.Query(collection, f)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.EqualValues(2, recordset.ResultCount)
		}

		// onesies
		for _, qs := range []string{
			`id/1`,
			`name/First`,
			`name/like:first`,
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
			recordset, err = search.Query(collection, f)
			assert.Nil(err)
			assert.NotNil(recordset, qs)
			assert.EqualValues(1, recordset.ResultCount, "%v", recordset.Records)
			record, ok = recordset.GetRecord(0)
			assert.True(ok)
			assert.NotNil(record, qs)
			assert.EqualValues(1, record.ID, qs)
			assert.Equal(`First`, record.Get(`name`), qs)
		}

		// nonesies
		for _, qs := range []string{
			`name/contains:irs/name/prefix:sec`,
		} {
			t.Logf("Querying (want 0 results): %q\n", qs)
			f, err := filter.Parse(qs)
			assert.Nil(err)
			recordset, err = search.Query(collection, f)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.EqualValues(0, recordset.ResultCount)
			assert.True(recordset.IsEmpty())
		}
	}
}

func testSearchQueryPaginated(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	collection := dal.NewCollection(`TestSearchQueryPaginated`)

	// set the global page size at the package level for this test
	backends.IndexerPageSize = 5

	if search := backend.WithSearch(collection); search != nil {
		err := backend.CreateCollection(collection)

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

		f := filter.All()
		f.Limit = 25

		recordset, err := search.Query(collection, f)
		assert.Nil(err)

		assert.NotNil(recordset)
		assert.Equal(21, len(recordset.Records))

		if recordset.KnownSize {
			assert.Equal(int64(21), recordset.ResultCount)
			assert.Equal(1, recordset.TotalPages)
		}
	}
}

func testSearchQueryLimit(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	backends.IndexerPageSize = 100
	c := dal.NewCollection(`TestSearchQueryLimit`)

	if search := backend.WithSearch(c); search != nil {
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

		recordset, err := search.Query(c, f)
		assert.Nil(err)
		assert.NotNil(recordset)

		assert.Equal(9, len(recordset.Records))

		if recordset.KnownSize {
			assert.Equal(int64(21), recordset.ResultCount)
			assert.Equal(3, recordset.TotalPages)
		}

		record, ok := recordset.GetRecord(0)
		assert.True(ok)
		assert.NotNil(record)
		assert.Equal(`00`, record.ID)
	}
}

func testSearchQueryOffset(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	backends.IndexerPageSize = 100
	c := dal.NewCollection(`TestSearchQueryOffset`)

	if search := backend.WithSearch(c); search != nil {
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

		f.Limit = 100
		f.Offset = 20

		recordset, err := search.Query(c, f)
		assert.Nil(err)
		assert.NotNil(recordset)
		assert.Equal(1, len(recordset.Records))

		if recordset.KnownSize {
			assert.Equal(int64(21), recordset.ResultCount)
			assert.Equal(1, recordset.TotalPages)
		}

		record, ok := recordset.GetRecord(0)
		assert.True(ok)
		assert.NotNil(record)
		assert.EqualValues(`20`, record.ID)
	}
}

func testSearchQueryOffsetLimit(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	c := dal.NewCollection(`TestSearchQueryOffsetLimit`)

	if search := backend.WithSearch(c); search != nil {
		old := backends.IndexerPageSize
		backends.IndexerPageSize = 3

		defer func() {
			backends.IndexerPageSize = old
		}()

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

		recordset, err := search.Query(c, f)
		assert.Nil(err)
		assert.NotNil(recordset)
		assert.Equal(9, len(recordset.Records))

		if recordset.KnownSize {
			assert.Equal(int64(21), recordset.ResultCount)
			assert.Equal(3, recordset.TotalPages)
		}

		record, ok := recordset.GetRecord(0)
		assert.True(ok)
		assert.NotNil(record)
		assert.Equal(`03`, record.ID)
	}
}

func testCompositeKeyQueries(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	collection := &dal.Collection{
		Name:              `TestCompositeKeyQueries`,
		IdentityFieldType: dal.StringType,
		Fields: []dal.Field{
			{
				Name: `other_id`,
				Type: dal.IntType,
				Key:  true,
			}, dal.Field{
				Name: `group`,
				Type: dal.StringType,
			},
		},
	}

	if search := backend.WithSearch(collection); search != nil {
		err := backend.CreateCollection(collection)

		defer func() {
			assert.Nil(backend.DeleteCollection(`TestCompositeKeyQueries`))
		}()

		assert.Nil(err)

		assert.Nil(backend.Insert(`TestCompositeKeyQueries`, dal.NewRecordSet(
			dal.NewRecord(`a`).SetFields(map[string]interface{}{
				`other_id`: 1,
				`group`:    `first`,
			}),
			dal.NewRecord(`a`).SetFields(map[string]interface{}{
				`other_id`: 2,
				`group`:    `second`,
			}),
			dal.NewRecord(`b`).SetFields(map[string]interface{}{
				`other_id`: 1,
				`group`:    `third`,
			}))))

		// test exact match with composite key
		f, err := filter.Parse(`id/a/other_id/1`)
		assert.Nil(err)

		recordset, err := search.Query(collection, f)
		assert.Nil(err)
		assert.NotNil(recordset)

		assert.EqualValues(1, recordset.ResultCount, "%v", recordset.Records)
		record, ok := recordset.GetRecord(0)
		assert.True(ok)
		assert.NotNil(record)
		assert.EqualValues(`a`, record.ID)
		assert.EqualValues(1, record.Get(`other_id`))

		// test scanning the primary key
		f, err = filter.Parse(`id/a`)
		assert.Nil(err)

		recordset, err = search.Query(collection, f)
		assert.Nil(err)
		assert.NotNil(recordset)

		assert.EqualValues(2, recordset.ResultCount, "%v", recordset.Records)
		assert.Equal([]interface{}{int64(1), int64(2)}, recordset.Pluck(`other_id`))
	}
}

func testListValues(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	collection := dal.NewCollection(`TestListValues`).
		AddFields(dal.Field{
			Name: `name`,
			Type: dal.StringType,
		}, dal.Field{
			Name: `group`,
			Type: dal.StringType,
		})

	if search := backend.WithSearch(collection); search != nil {
		err := backend.CreateCollection(collection)

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

		keyValues, err := search.ListValues(collection, []string{`name`}, filter.All())
		assert.Nil(err)
		assert.Equal(1, len(keyValues))
		v, ok := keyValues[`name`]
		assert.True(ok)
		assert.ElementsMatch([]interface{}{`first`, `second`, `third`}, v)

		keyValues, err = search.ListValues(collection, []string{`group`}, filter.All())
		assert.Nil(err)
		assert.Equal(1, len(keyValues))
		v, ok = keyValues[`group`]
		assert.True(ok)
		assert.ElementsMatch([]interface{}{`reds`, `blues`}, v)

		keyValues, err = search.ListValues(collection, []string{`id`}, filter.All())
		assert.Nil(err)
		assert.Equal(1, len(keyValues))
		v, ok = keyValues[`id`]
		assert.True(ok)
		assert.ElementsMatch([]interface{}{int64(1), int64(2), int64(3)}, v)

		keyValues, err = search.ListValues(collection, []string{`id`, `group`}, filter.All())
		assert.Nil(err)
		assert.Equal(2, len(keyValues))

		v, ok = keyValues[`id`]
		assert.True(ok)
		assert.ElementsMatch([]interface{}{int64(1), int64(2), int64(3)}, v)

		v, ok = keyValues[`group`]
		assert.True(ok)
		assert.ElementsMatch([]interface{}{`reds`, `blues`}, v)
	}
}

func testSearchAnalysis(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	collection := dal.NewCollection(`TestSearchAnalysis`).
		AddFields(dal.Field{
			Name: `single`,
			Type: dal.StringType,
		}, dal.Field{
			Name: `char_filter_test`,
			Type: dal.StringType,
		})

	if search := backend.WithSearch(collection); search != nil {
		err := backend.CreateCollection(collection)

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
			`char_filter_test/like:this result`,
		} {
			t.Logf("Querying (want 3 results): %q\n", qs)
			f, err := filter.Parse(qs)
			assert.Nil(err)
			recordset, err := search.Query(collection, f)
			assert.Nil(err)
			assert.NotNil(recordset)
			assert.EqualValues(3, recordset.ResultCount, "%v", recordset.Records)
		}
	}
}

func testObjectType(t *testing.T, backend backends.Backend) {
	assert := require.New(t)

	err := backend.CreateCollection(
		dal.NewCollection(`TestObjectType`).
			AddFields(dal.Field{
				Name: `properties`,
				Type: dal.ObjectType,
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
	assert.EqualValues(1, record.GetNested(`properties.count`))
}

func testAggregators(t *testing.T, backend backends.Backend) {
	assert := require.New(t)
	collection := dal.NewCollection(`TestAggregators`).
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
		})

	err := backend.CreateCollection(collection)

	defer func() {
		assert.NoError(backend.DeleteCollection(`TestAggregators`))
	}()

	assert.NoError(err)

	if agg := backend.WithAggregator(collection); agg != nil {
		// Insert and Retrieve
		// --------------------------------------------------------------------------------------------
		assert.NoError(backend.Insert(`TestAggregators`, dal.NewRecordSet(
			dal.NewRecord(1).Set(`color`, `red`).Set(`inventory`, 34).Set(`factor`, float64(2.7)).Set(`created_at`, time.Now()),
			dal.NewRecord(2).Set(`color`, `green`).Set(`inventory`, 92).Set(`factor`, float64(9.8)).Set(`created_at`, time.Now()),
			dal.NewRecord(3).Set(`color`, `blue`).Set(`inventory`, 0).Set(`factor`, float64(5.6)).Set(`created_at`, time.Now()),
			dal.NewRecord(4).Set(`color`, `orange`).Set(`inventory`, 54).Set(`factor`, float64(0)).Set(`created_at`, time.Now()),
			dal.NewRecord(5).Set(`color`, `yellow`).Set(`inventory`, 123).Set(`factor`, float64(3.14)).Set(`created_at`, time.Now()),
			dal.NewRecord(6).Set(`color`, `gold`).Set(`inventory`, 19).Set(`factor`, float64(4.67)).Set(`created_at`, time.Now()),
		)))

		vui, err := agg.Count(collection, filter.All())
		assert.NoError(err)
		assert.Equal(uint64(6), vui)

		vf, err := agg.Sum(collection, `inventory`, filter.All())
		assert.NoError(err)
		assert.Equal(float64(322), vf)

		vf, err = agg.Minimum(collection, `inventory`, filter.All())
		assert.NoError(err)
		assert.Equal(float64(0), vf)

		vf, err = agg.Minimum(collection, `factor`, filter.All())
		assert.NoError(err)
		assert.Equal(float64(0), vf)

		vf, err = agg.Maximum(collection, `inventory`, filter.All())
		assert.NoError(err)
		assert.Equal(float64(123), vf)

		vf, err = agg.Maximum(collection, `factor`, filter.All())
		assert.NoError(err)
		assert.Equal(float64(9.8), vf)
	}
}

func testModelCRUD(t *testing.T, db backends.Backend) {
	assert := require.New(t)

	type ModelOne struct {
		ID      int
		Name    string               `pivot:"name"`
		Enabled bool                 `pivot:"enabled,omitempty"`
		Type    testTypeWithStringer `pivot:"type"`
		Size    int                  `pivot:"size,omitempty"`
	}

	model1 := mapper.NewModel(db, &dal.Collection{
		Name: `testModelCRUD`,
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
	err := model1.Get(1, v)

	assert.Nil(err)
	assert.Equal(1, v.ID)
	assert.Equal(`Test1`, v.Name)
	assert.Equal(true, v.Enabled)
	assert.Equal(12345, v.Size)
	// assert.EqualValues(TestSecond, v.Type) // TODO: fix this

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
	// assert.Equal(TestThird, v.Type) // TODO: fix this

	assert.Nil(model1.Delete(1))
	assert.Error(model1.Get(1, nil))
	assert.Nil(model1.Drop())
}

func testModelFind(t *testing.T, db backends.Backend) {
	assert := require.New(t)

	type ModelTwoPropItem struct {
		Name  string
		Value int
	}

	type ModelTwoProps []ModelTwoPropItem

	type ModelTwoConfig struct {
		ThingEnabled bool
		TestName     string
		ItemCount    int
		Properties   ModelTwoProps
	}

	type ModelTwo struct {
		ID      int
		Name    string         `pivot:"name"`
		Enabled bool           `pivot:"enabled,omitempty"`
		Size    int            `pivot:"size,omitempty"`
		Config  ModelTwoConfig `pivot:"config"`
	}

	model := mapper.NewModel(db, &dal.Collection{
		Name: `testModelFind`,
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
			{
				Name: `config`,
				Type: dal.ObjectType,
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
		Config: ModelTwoConfig{
			ThingEnabled: true,
			TestName:     `m2config`,
			ItemCount:    4,
			Properties: ModelTwoProps{
				{
					Name:  `aaa`,
					Value: 2,
				},
				{
					Name:  `bbb`,
					Value: 7,
				},
			},
		},
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
	assert.EqualValues([]ModelTwo{
		{
			ID:      1,
			Name:    `test-one`,
			Enabled: true,
			Size:    12345,
		}, {
			ID:      2,
			Name:    `test-two`,
			Enabled: false,
			Size:    98765,
			Config: ModelTwoConfig{
				ThingEnabled: true,
				TestName:     `m2config`,
				ItemCount:    4,
				Properties: ModelTwoProps{
					{
						Name:  `aaa`,
						Value: 2,
					},
					{
						Name:  `bbb`,
						Value: 7,
					},
				},
			},
		}, {
			ID:      3,
			Name:    `test-three`,
			Enabled: true,
		},
	}, resultsStruct)

	var recordset dal.RecordSet

	assert.Error(model.All(recordset))
	assert.NoError(model.All(&recordset))
	assert.Equal(int64(3), recordset.ResultCount)
	assert.Nil(model.Drop())
}

func testModelList(t *testing.T, db backends.Backend) {
	assert := require.New(t)

	type ModelTwo struct {
		ID      int
		Name    string `pivot:"name"`
		Enabled bool   `pivot:"enabled,omitempty"`
		Size    int    `pivot:"size,omitempty"`
	}

	model := mapper.NewModel(db, &dal.Collection{
		Name: `testModelList`,
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
		Name:    `test-1`,
		Enabled: true,
		Size:    12345,
	}))

	assert.Nil(model.Create(&ModelTwo{
		ID:      2,
		Name:    `test-2`,
		Enabled: false,
		Size:    98765,
	}))

	assert.Nil(model.Create(&ModelTwo{
		ID:      3,
		Name:    `test-3`,
		Enabled: true,
	}))

	values, err := model.List([]string{`name`})
	assert.Nil(err)
	assert.EqualValues([]interface{}{
		`test-1`,
		`test-2`,
		`test-3`,
	}, values[`name`])

	values, err = model.List([]string{`name`, `size`})
	assert.Nil(err)
	assert.EqualValues([]interface{}{
		`test-1`,
		`test-2`,
		`test-3`,
	}, values[`name`])

	// FIXME: really need to work out where we come down on "0"
	// assert.EqualValues([]interface{}{
	// 	int64(0),
	// 	int64(12345),
	// 	int64(98765),
	// }, values[`size`])
}

func testLoadSchema(t *testing.T, db backends.Backend) {
	assert := require.New(t)
	assert.NoError(ApplySchemata(`./test/schema/`, db))
}

func testLoadFixtures(t *testing.T, db backends.Backend) {
	assert := require.New(t)
	assert.NoError(LoadFixtures(`./test/fixtures/`, db))

	if db.Supports(backends.Constraints) {
		assert.NotNil(db.Insert(`users`, dal.NewRecordSet(dal.NewRecord(`testXYZ`, map[string]interface{}{
			`FirstName`:      `ShouldNot`,
			`LastName`:       `SeeMe`,
			`PrimaryGroupID`: `nonexistent`,
			`PasswordHash`:   `$`,
			`Salt`:           `abc`,
		}))))
	}
}
