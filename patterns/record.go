package patterns

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/util"
	"io/ioutil"
	"net/http"
)

type IRecordAccessPattern interface {
	RequestToFilter(*http.Request, map[string]string) (filter.Filter, error)
	GetStatus() map[string]interface{}
	ReadDatasetSchema() *dal.Dataset
	ReadCollectionSchema(collectionName string) (dal.Collection, bool)
	UpdateCollectionSchema(action dal.CollectionAction, collectionName string, schema dal.Collection) error
	DeleteCollectionSchema(collectionName string) error
	GetRecords(collectionName string, query filter.Filter) (*dal.RecordSet, error)
	InsertRecords(collectionName string, query filter.Filter, records *dal.RecordSet) error
	UpdateRecords(collectionName string, query filter.Filter, records *dal.RecordSet) error
	DeleteRecords(collectionName string, query filter.Filter) error
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
						return http.StatusNotFound, nil, fmt.Errorf("Could not locate collection '%s'", collectionName)
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
				if collectionName, ok := params[`collection`]; ok {
					if action, ok := params[`action`]; ok {
						var schemaAction dal.CollectionAction

						switch action {
						case `verify`:
							schemaAction = dal.SchemaVerify
						case `create`:
							schemaAction = dal.SchemaCreate
						case `expand`:
							schemaAction = dal.SchemaExpand
						case `enforce`:
							schemaAction = dal.SchemaEnforce
						default:
							return http.StatusBadRequest, nil, fmt.Errorf("Unsupported action '%s'", action)
						}

						if data, err := ioutil.ReadAll(request.Body); err == nil {
							definition := dal.Collection{}

							if err := json.Unmarshal(data, &definition); err == nil {
								return http.StatusNoContent, nil, pattern.UpdateCollectionSchema(schemaAction, collectionName, definition)
							} else {
								return http.StatusBadRequest, nil, err
							}
						} else {
							return http.StatusBadRequest, nil, err
						}
					} else {
						return http.StatusBadRequest, nil, fmt.Errorf("Schema action not specified")
					}
				} else {
					return http.StatusBadRequest, nil, fmt.Errorf("Collection name not specified")
				}
			},
		},

		// Data Query & Manipulation
		// ---------------------------------------------------------------------------------------------
		{
			BackendName: backendName,
			Method:      `GET`,
			Path:        `/query/:collection/all`,
			Handler: func(request *http.Request, params map[string]string) (int, interface{}, error) {
				params[`query`] = `all`
				return handlerGetRecords(backendName, pattern)(request, params)
			},
		},
		{
			BackendName: backendName,
			Method:      `GET`,
			Path:        `/query/:collection/where/*query`,
			Handler:     handlerGetRecords(backendName, pattern),
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

func handlerGetRecords(backendName string, pattern IRecordAccessPattern) util.EndpointResponseFunc {
	return func(request *http.Request, params map[string]string) (int, interface{}, error) {
		if collectionName, ok := params[`collection`]; ok {
			if f, err := pattern.RequestToFilter(request, params); err == nil {
				if recordSet, err := pattern.GetRecords(collectionName, f); err == nil {
					return http.StatusOK, recordSet, nil
				} else {
					return http.StatusInternalServerError, recordSet, err
				}
			} else {
				return http.StatusBadRequest, nil, err
			}
		} else {
			return http.StatusBadRequest, nil, fmt.Errorf("Empty collection name specified")
		}
	}
}
