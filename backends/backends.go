package backends

import (
	"fmt"
	"time"

	"github.com/alexcesaro/statsd"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger(`pivot/backends`)
var querylog = logging.MustGetLogger(`pivot/querylog`)
var stats, _ = statsd.New()
var DefaultAutoregister = false

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
}

var NotImplementedError = fmt.Errorf("Not Implemented")

type BackendFunc func(dal.ConnectionString) Backend

var backendMap = map[string]BackendFunc{
	`dynamodb`:   NewDynamoBackend,
	`file`:       NewFilesystemBackend,
	`fs`:         NewFilesystemBackend,
	`mongodb`:    NewMongoBackend,
	`mysql`:      NewSqlBackend,
	`postgres`:   NewSqlBackend,
	`postgresql`: NewSqlBackend,
	`psql`:       NewSqlBackend,
	`sqlite`:     NewSqlBackend,
}

func RegisterBackend(name string, fn BackendFunc) {
	backendMap[name] = fn
}

func MakeBackend(connection dal.ConnectionString) (Backend, error) {
	backendName := connection.Backend()
	log.Infof("Creating backend: %v", connection.String())

	if fn, ok := backendMap[backendName]; ok {
		if backend := fn(connection); backend != nil {
			return backend, nil
		} else {
			return nil, fmt.Errorf("Error occurred instantiating backend %q", backendName)
		}
	} else {
		return nil, fmt.Errorf("Unknown backend type %q", backendName)
	}
}
