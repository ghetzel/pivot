package backends

import (
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type ResultFunc func(ptrToInstance any, err error) // {}

type Mapper interface {
	GetBackend() Backend
	GetCollection() *dal.Collection
	Migrate() error
	Drop() error
	Exists(id any) bool
	Create(from any) error
	Get(id any, into any) error
	Update(from any) error
	CreateOrUpdate(id any, from any) error
	Delete(ids ...any) error
	DeleteQuery(flt any) error
	Find(flt any, into any) error
	FindFunc(flt any, destZeroValue any, resultFn ResultFunc) error
	All(into any) error
	Each(destZeroValue any, resultFn ResultFunc) error
	List(fields []string) (map[string][]any, error)
	ListWithFilter(fields []string, flt any) (map[string][]any, error)
	Sum(field string, flt any) (float64, error)
	Count(flt any) (uint64, error)
	Minimum(field string, flt any) (float64, error)
	Maximum(field string, flt any) (float64, error)
	Average(field string, flt any) (float64, error)
	GroupBy(fields []string, aggregates []filter.Aggregate, flt any) (*dal.RecordSet, error)
}
