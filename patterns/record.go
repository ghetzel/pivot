package patterns

import (
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/util"
	"net/http"
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

func registerRecordAccessPatternHandlers(backendName string, pattern IRecordAccessPattern, backendI interface{}) ([]util.Endpoint, error) {
	return []util.Endpoint{
		// Schema Control
		// ---------------------------------------------------------------------------------------------
		{
			BackendName: backendName,
			Method:      `GET`,
			Path:        `/`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusOK, pattern.GetStatus(), nil
			},
		},
		{
			BackendName: backendName,
			Method:      `GET`,
			Path:        `/schema`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusOK, pattern.ReadDatasetSchema(), nil
			},
		},
		{
			BackendName: backendName,
			Method:      `GET`,
			Path:        `/schema/:collection`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				if collectionName, ok := params[`collection`]; ok {
					if collection, ok := pattern.ReadCollectionSchema(collectionName); ok {
						return http.StatusOK, collection, nil
					} else {
						return http.StatusNotFound, nil, fmt.Errorf("Could not locate collection %q", collectionName)
					}
				} else {
					return http.StatusBadRequest, nil, fmt.Errorf("Empty collection name specified")
				}
			},
		},
		{
			BackendName: backendName,
			Method:      `DELETE`,
			Path:        `/schema/:collection`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				if collectionName, ok := params[`collection`]; ok {
					return http.StatusOK, nil, pattern.DeleteCollectionSchema(collectionName)
				} else {
					return http.StatusBadRequest, nil, fmt.Errorf("Empty collection name specified")
				}
			},
		},
		{
			BackendName: backendName,
			Method:      `PUT`,
			Path:        `/schema/:collection/:action`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusNotImplemented, nil, fmt.Errorf("NI: [%s].UpdateCollectionSchema()", backendName)
			},
		},

		// Data Query & Manipulation
		// ---------------------------------------------------------------------------------------------
		{
			BackendName: backendName,
			Method:      `GET`,
			Path:        `/query/:collection/all`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusNotImplemented, nil, fmt.Errorf("NI: [%s].GetAllRecords()", backendName)
			},
		},
		{
			BackendName: backendName,
			Method:      `GET`,
			Path:        `/query/:collection/where/*query`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusNotImplemented, nil, fmt.Errorf("NI: [%s].GetRecords()", backendName)
			},
		},
		{
			BackendName: backendName,
			Method:      `POST`,
			Path:        `/query/:collection/where/*query`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusNotImplemented, nil, fmt.Errorf("NI: [%s].InsertRecords()", backendName)
			},
		},
		{
			BackendName: backendName,
			Method:      `PUT`,
			Path:        `/query/:collection/where/*query`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusNotImplemented, nil, fmt.Errorf("NI: [%s].UpdateRecords()", backendName)
			},
		},
		{
			BackendName: backendName,
			Method:      `DELETE`,
			Path:        `/query/:collection/where/*query`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusNotImplemented, nil, fmt.Errorf("NI: [%s].DeleteRecords()", backendName)
			},
		},
		{
			BackendName: backendName,
			Method:      `POST`,
			Path:        `/query/:collection`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				return http.StatusNotImplemented, nil, fmt.Errorf("NI: [%s].InsertRecords()", backendName)
			},
		},
	}, nil
}
