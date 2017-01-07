package backends

import (
	"fmt"
	"github.com/alexcesaro/statsd"
	"github.com/ghetzel/pivot/dal"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger(`backends`)
var stats, _ = statsd.New()

type Backend interface {
	Initialize() error
	SetOptions(ConnectOptions)
	GetConnectionString() *dal.ConnectionString
	Exists(collection string, id interface{}) bool
	Retrieve(collection string, id interface{}, fields ...string) (*dal.Record, error)
	Insert(collection string, records *dal.RecordSet) error
	Update(collection string, records *dal.RecordSet, target ...string) error
	Delete(collection string, ids ...interface{}) error
	CreateCollection(definition dal.Collection) error
	DeleteCollection(collection string) error
	ListCollections() ([]string, error)
	GetCollection(collection string) (*dal.Collection, error)
	WithSearch() Indexer
}

func MakeBackend(connection dal.ConnectionString) (Backend, error) {
	log.Debugf("Creating backend for connection string %q", connection.String())

	switch connection.Backend() {
	case `boltdb`:
		return NewBoltBackend(connection), nil
	case `sqlite`, `mysql`:
		return NewSqlBackend(connection), nil
	default:
		return nil, fmt.Errorf("Unknown backend type %q", connection.Backend())
	}
}
