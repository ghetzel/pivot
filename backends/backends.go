package backends

import (
	"fmt"

	"github.com/alexcesaro/statsd"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger(`pivot/backends`)
var querylog = logging.MustGetLogger(`pivot/querylog`)
var stats, _ = statsd.New()

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
	WithSearch(collection string, filters ...*filter.Filter) Indexer
	WithAggregator(collection string) Aggregator
	Flush() error
}

var NotImplementedError = fmt.Errorf("Not Implemented")

type BackendFunc func(dal.ConnectionString) Backend

var backendMap = map[string]BackendFunc{
	`sqlite`:   NewSqlBackend,
	`mysql`:    NewSqlBackend,
	`postgres`: NewSqlBackend,
	`fs`:       NewFilesystemBackend,
	`file`:     NewFilesystemBackend,
	`tiedot`:   NewTiedotBackend,
	`dynamodb`: NewDynamoBackend,
}

func RegisterBackend(name string, fn BackendFunc) {
	backendMap[name] = fn
}

func MakeBackend(connection dal.ConnectionString) (Backend, error) {
	backendName := connection.Backend()
	log.Debugf("Creating backend for connection string %q", connection.String())

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
