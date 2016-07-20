package patterns

import (
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type IRecordAccessPattern interface {
	GetStatus() map[string]interface{}
	ReadDatasetSchema() *dal.Dataset
	ReadCollectionSchema(string) (dal.Collection, bool)
	UpdateCollectionSchema(dal.CollectionAction, string, dal.Collection) error
	DeleteCollectionSchema(string) error
	GetRecords(string, filter.Filter) (*dal.RecordSet, error)
	InsertRecords(string, filter.Filter, *dal.RecordSet) error
	UpdateRecords(string, filter.Filter, *dal.RecordSet) error
	DeleteRecords(string, filter.Filter) error
}
