package backends

import (
	"fmt"
	"time"

	"github.com/alexcesaro/statsd"
	"github.com/ghetzel/go-stockutil/log"
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
	Constraints
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
	`dynamodb`:      NewDynamoBackend,
	`file`:          NewFileBackend,
	`fs`:            NewFilesystemBackend,
	`mongodb`:       NewMongoBackend,
	`mongo`:         NewMongoBackend,
	`mysql`:         NewSqlBackend,
	`postgres`:      NewSqlBackend,
	`postgresql`:    NewSqlBackend,
	`psql`:          NewSqlBackend,
	`sqlite`:        NewSqlBackend,
	`redis`:         NewRedisBackend,
	`elasticsearch`: NewElasticsearchBackend,
	`es`:            NewElasticsearchBackend,
}

// Register a new or replacement backend for the given connection string scheme.
// For example, registering backend "foo" will allow Pivot to handle "foo://"
// connection strings.
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

// Instantiate the appropriate Backend for the given connection string.
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
