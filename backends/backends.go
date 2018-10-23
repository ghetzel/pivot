package backends

import (
	"fmt"
	"strings"
	"time"

	"github.com/alexcesaro/statsd"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

var querylog = log.Logger()
var stats, _ = statsd.New()
var DefaultAutoregister = false
var AutopingTimeout = 5 * time.Second

type BackendFeature int

const (
	PartialSearch BackendFeature = iota
	CompositeKeys
)

type Backend interface {
	Initialize() error
	SetIndexer(dal.ConnectionString) error
	RegisterCollection(*dal.Collection)
	GetConnectionString() *dal.ConnectionString
	Exists(collection string, id interface{}) bool
	Retrieve(collection string, id interface{}, fields ...string) (*dal.Record, error)
	Insert(collection string, records *dal.RecordSet) error
	Update(collection string, records *dal.RecordSet, target ...string) error
	Delete(collection string, ids ...interface{}) error
	CreateCollection(definition *dal.Collection) error
	DeleteCollection(collection string) error
	ListCollections() ([]string, error)
	GetCollection(collection string) (*dal.Collection, error)
	WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer
	WithAggregator(collection *dal.Collection) Aggregator
	Flush() error
	Ping(time.Duration) error
	String() string
	Supports(feature ...BackendFeature) bool
}

var NotImplementedError = fmt.Errorf("Not Implemented")

type BackendFunc func(dal.ConnectionString) Backend

var backendMap = map[string]BackendFunc{
	`dynamodb`:   NewDynamoBackend,
	`file`:       NewFilesystemBackend,
	`fs`:         NewFilesystemBackend,
	`mongodb`:    NewMongoBackend,
	`mongo`:      NewMongoBackend,
	`mysql`:      NewSqlBackend,
	`postgres`:   NewSqlBackend,
	`postgresql`: NewSqlBackend,
	`psql`:       NewSqlBackend,
	`sqlite`:     NewSqlBackend,
	`redis`:      NewRedisBackend,
}

func RegisterBackend(name string, fn BackendFunc) {
	backendMap[name] = fn
}

func startPeriodicPinger(interval time.Duration, backend Backend) {
	for {
		if err := backend.Ping(AutopingTimeout); err != nil {
			log.Warningf("%v: ping failed with error: %v", backend, err)
		}

		time.Sleep(interval)
	}
}

func MakeBackend(connection dal.ConnectionString) (Backend, error) {
	var autopingInterval time.Duration

	backendName := connection.Backend()
	log.Infof("Creating backend: %v", connection.String())

	if fn, ok := backendMap[backendName]; ok {
		if i := connection.OptDuration(`ping`, 0); i > 0 {
			autopingInterval = i
		}

		connection.ClearOpt(`ping`)

		if backend := fn(connection); backend != nil {
			if autopingInterval > 0 {
				go startPeriodicPinger(autopingInterval, backend)
			}

			return backend, nil
		} else {
			return nil, fmt.Errorf("Error occurred instantiating backend %q", backendName)
		}
	} else {
		return nil, fmt.Errorf("Unknown backend type %q", backendName)
	}
}

func InflateEmbeddedRecords(backend Backend, parent *dal.Collection, record *dal.Record, prepId func(interface{}) interface{}) error { // for each relationship
	skipKeys := make([]string, 0)

	if embed, ok := backend.(*EmbeddedRecordBackend); ok {
		skipKeys = embed.SkipKeys
	}

	for _, relationship := range parent.EmbeddedCollections {
		keys := sliceutil.CompactString(sliceutil.Stringify(sliceutil.Sliceify(relationship.Keys)))

		// if we're supposed to skip certain keys, and this is one of them
		if len(skipKeys) > 0 && sliceutil.ContainsAnyString(skipKeys, keys...) {
			log.Debugf("explicitly skipping %+v", keys)
			continue
		}

		var related *dal.Collection

		if relationship.Collection != nil {
			related = relationship.Collection
		} else if c, err := backend.GetCollection(relationship.CollectionName); c != nil {
			related = c
		} else {
			return fmt.Errorf("error in relationship %v: %v", keys, err)
		}

		if related.Name == parent.Name {
			log.Debugf("not descending into %v to avoid loop", related.Name)
			continue
		}

		var nestedFields []string

		// determine fields in final output handling
		// a. no exported fields                      -> use relationship fields
		// b. exported fields, no relationship fields -> use exported fields
		// c. both relationship and exported fields   -> relfields ∩ exported
		//
		relfields := relationship.Fields
		exported := related.ExportedFields

		if len(exported) == 0 {
			nestedFields = relfields
		} else if len(relfields) == 0 {
			nestedFields = exported
		} else {
			nestedFields = sliceutil.IntersectStrings(relfields, exported)
		}

		for _, key := range keys {
			keyBefore, _ := stringutil.SplitPair(key, `.*`)

			if nestedId := record.Get(key); nestedId != nil {
				if typeutil.IsArray(nestedId) {
					results := make([]map[string]interface{}, 0)

					for _, id := range sliceutil.Sliceify(nestedId) {
						if prepId != nil {
							id = prepId(id)
						}

						if data, err := retrieveEmbeddedRecord(backend, parent, related, id, nestedFields...); err == nil {
							results = append(results, data)
						} else {
							return err
						}
					}

					// clear out the array we're modifying
					record.SetNested(keyBefore, []interface{}{})

					for i, result := range results {
						if len(result) > 0 {
							nestKey := strings.Replace(key, `*`, fmt.Sprintf("%d", i), 1)
							record.SetNested(nestKey, result)
						}
					}

				} else {
					if prepId != nil {
						nestedId = prepId(nestedId)
					}

					if data, err := retrieveEmbeddedRecord(backend, parent, related, nestedId, nestedFields...); err == nil {
						if len(data) > 0 {
							record.SetNested(keyBefore, data)
						}
					} else {
						return err
					}
				}
			}
		}
	}

	return nil
}

func retrieveEmbeddedRecord(backend Backend, parent *dal.Collection, related *dal.Collection, id interface{}, fields ...string) (map[string]interface{}, error) {
	if id == nil {
		return nil, nil
	}

	// retrieve the record by ID
	if record, err := backend.Retrieve(related.Name, id, fields...); err == nil {

		if data, err := related.MapFromRecord(record, fields...); err == nil {
			return data, nil
		} else if parent.AllowMissingEmbeddedRecords {
			// log.Warningf("nested(%s[%s]): %v", parent.Name, related.Name, err)
			return nil, nil
		} else {
			return nil, fmt.Errorf("nested(%s[%s]): serialization error: %v", parent.Name, related.Name, err)
		}
	} else if parent.AllowMissingEmbeddedRecords {
		// if dal.IsNotExistError(err) {
		// 	log.Warningf("nested(%s[%s]): record %v is missing", parent.Name, related.Name, id)
		// } else {
		// 	log.Warningf("nested(%s[%s]): retrieval error on %v: %v", parent.Name, related.Name, id, err)
		// }

		return nil, nil
	} else {
		return nil, fmt.Errorf("nested(%s[%s]): %v", parent.Name, related.Name, err)
	}
}
