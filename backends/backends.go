package backends

import (
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type Backend interface {
	Initialize() error
	InsertRecords(collection string, records *dal.RecordSet) error
	GetRecordById(collection string, id dal.Identity) (*dal.Record, error)
	UpdateRecords(collection string, records *dal.RecordSet) error
	DeleteRecords(collection string, id []dal.Identity) error
	CreateCollection(definition dal.Collection) error
	DeleteCollection(collection string) error
	GetCollection(collection string) (dal.Collection, error)
	Query(collection string, filter filter.Filter) (*dal.RecordSet, error)
}

func MakeBackend(connection dal.ConnectionString) (Backend, error) {
	switch connection.Backend() {
	case `bolt`:
		return NewBoltBackend(connection), nil
	default:
		return nil, fmt.Errorf("Unknown backend type %q", connection.Backend())
	}
}
