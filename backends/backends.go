package backends

import (
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger(`backends`)

type Searchable interface {
	Query(collection string, filter filter.Filter) (*dal.RecordSet, error)
	QueryString(collection string, filterString string) (*dal.RecordSet, error)
}

type Backend interface {
	Initialize() error
	InsertRecords(collection string, records *dal.RecordSet) error
	GetRecordById(collection string, id dal.Identity) (*dal.Record, error)
	UpdateRecords(collection string, records *dal.RecordSet) error
	DeleteRecords(collection string, id []dal.Identity) error
	CreateCollection(definition dal.Collection) error
	DeleteCollection(collection string) error
	GetCollection(collection string) (dal.Collection, error)
	WithSearch() Searchable
}

func MakeBackend(connection dal.ConnectionString) (Backend, error) {
	log.Debugf("Creating backend for connection string %q", connection.String())

	switch connection.Backend() {
	case `bolt`:
		return NewBoltBackend(connection), nil
	default:
		return nil, fmt.Errorf("Unknown backend type %q", connection.Backend())
	}
}

type AbstractSearchable struct {
	Searchable
}

func (self AbstractSearchable) QueryString(collection string, filterString string) (*dal.RecordSet, error) {
	if f, err := filter.Parse(filterString); err == nil {
		return self.Query(collection, f)
	}else{
		return nil, err
	}
}
