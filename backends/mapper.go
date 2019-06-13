package backends

import (
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type ResultFunc func(ptrToInstance interface{}, err error) // {}

type Mapper interface {
	GetBackend() Backend
	GetCollection() *dal.Collection
	Migrate() error
	Drop() error
	Exists(id interface{}) bool
	Create(from interface{}) error
	Get(id interface{}, into interface{}) error
	Update(from interface{}) error
	CreateOrUpdate(id interface{}, from interface{}) error
	Delete(ids ...interface{}) error
	DeleteQuery(flt interface{}) error
	Find(flt interface{}, into interface{}) error
	FindFunc(flt interface{}, destZeroValue interface{}, resultFn ResultFunc) error
	All(into interface{}) error
	Each(destZeroValue interface{}, resultFn ResultFunc) error
	List(fields []string) (map[string][]interface{}, error)
	ListWithFilter(fields []string, flt interface{}) (map[string][]interface{}, error)
	Sum(field string, flt interface{}) (float64, error)
	Count(flt interface{}) (uint64, error)
	Minimum(field string, flt interface{}) (float64, error)
	Maximum(field string, flt interface{}) (float64, error)
	Average(field string, flt interface{}) (float64, error)
	GroupBy(fields []string, aggregates []filter.Aggregate, flt interface{}) (*dal.RecordSet, error)
}
