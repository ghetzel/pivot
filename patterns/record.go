package patterns

import (
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
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

func registerRecordAccessPatternHandlers(mux *http.ServeMux, backendName string, pattern IRecordAccessPattern) error {
	log.Debugf("Setting up routes for backend %q (record access pattern)", backendName)

	// Schema Control
	// ---------------------------------------------------------------------------------------------
	mux.HandleFunc(urlForBackend(backendName), func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case `GET`:
			// GetStatus()
			http.Error(w, fmt.Sprintf("NI: [%s].GetStatus()", backendName), http.StatusNotImplemented)
		}
	})

	mux.HandleFunc(urlForBackend(backendName, `schema`), func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case `GET`:
			// ReadDatasetSchema()
			http.Error(w, fmt.Sprintf("NI: [%s].ReadDatasetSchema()", backendName), http.StatusNotImplemented)
		}
	})

	mux.HandleFunc(urlForBackend(backendName, `schema`, `:collection`), func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case `GET`:
			// ReadCollectionSchema()
			http.Error(w, fmt.Sprintf("NI: [%s].ReadCollectionSchema()", backendName), http.StatusNotImplemented)

		case `DELETE`:
			// DeleteCollectionSchema()
			http.Error(w, fmt.Sprintf("NI: [%s].DeleteCollectionSchema()", backendName), http.StatusNotImplemented)
		}
	})

	mux.HandleFunc(urlForBackend(backendName, `schema`, `:collection`, `:action`), func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case `PUT`:
			// UpdateCollectionSchema()
			http.Error(w, fmt.Sprintf("NI: [%s].UpdateCollectionSchema()", backendName), http.StatusNotImplemented)
		}
	})

	// Data Query & Manipulation
	// ---------------------------------------------------------------------------------------------
	mux.HandleFunc(urlForBackend(backendName, `query`, `:collection`, `all`), func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case `GET`:
			// GetAllRecords()
			http.Error(w, fmt.Sprintf("NI: [%s].GetAllRecords()", backendName), http.StatusNotImplemented)
		}
	})

	mux.HandleFunc(urlForBackend(backendName, `query`, `:collection`, `where`, `*query`), func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case `GET`:
			// GetRecords()
			http.Error(w, fmt.Sprintf("NI: [%s].GetRecords()", backendName), http.StatusNotImplemented)

		case `POST`:
			// InsertRecords()
			http.Error(w, fmt.Sprintf("NI: [%s].InsertRecords()", backendName), http.StatusNotImplemented)

		case `PUT`:
			// UpdateRecords()
			http.Error(w, fmt.Sprintf("NI: [%s].UpdateRecords()", backendName), http.StatusNotImplemented)

		case `DELETE`:
			// DeleteRecords()
			http.Error(w, fmt.Sprintf("NI: [%s].DeleteRecords()", backendName), http.StatusNotImplemented)
		}
	})

	mux.HandleFunc(urlForBackend(backendName, `query`, `:collection`), func(w http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case `POST`:
			http.Error(w, fmt.Sprintf("NI: [%s].InsertRecords()", backendName), http.StatusNotImplemented)
		}
	})

	return nil
}
