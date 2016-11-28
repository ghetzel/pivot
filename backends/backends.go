package backends

import (
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger(`backends`)

type Backend interface {
	Initialize() error
	GetConnectionString() *dal.ConnectionString
	InsertRecords(collection string, records *dal.RecordSet) error
	GetRecordById(collection string, id string) (*dal.Record, error)
	UpdateRecords(collection string, records *dal.RecordSet) error
	DeleteRecords(collection string, ids []string) error
	CreateCollection(definition dal.Collection) error
	DeleteCollection(collection string) error
	GetCollection(collection string) (dal.Collection, error)
	WithSearch() Indexer
}

func MakeBackend(connection dal.ConnectionString) (Backend, error) {
	log.Debugf("Creating backend for connection string %q", connection.String())

	switch connection.Backend() {
	case `boltdb`:
		return NewBoltBackend(connection), nil
	default:
		return nil, fmt.Errorf("Unknown backend type %q", connection.Backend())
	}
}
