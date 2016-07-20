package pivot

import (
	"fmt"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/patterns"
)

// GetStatus() map[string]interface{}
// ReadDatasetSchema() *dal.Dataset
// ReadCollectionSchema(string) (dal.Collection, bool)
// UpdateCollectionSchema(dal.CollectionAction, string, dal.Collection) error
// DeleteCollectionSchema(string) error
// GetRecords(string, filter.Filter) (*dal.RecordSet, error)
// InsertRecords(string, filter.Filter, *dal.RecordSet) error
// UpdateRecords(string, filter.Filter, *dal.RecordSet) error
// DeleteRecords(string, filter.Filter) error

func (self *Server) setupBackendRoutes() error {
	for name, backend := range Backends {
		log.Debugf("Registering routes for backend: %q", name)

		if err := self.registerBackendRoutes(backend); err != nil {
			return err
		}
	}

	return nil
}

func (self *Server) registerBackendRoutes(b interface{}) error {
	switch b.(type) {
	case backends.IBackend:
		backend := b.(backends.IBackend)

		if err := patterns.RegisterHandlers(self.mux, backend.GetName(), b); err != nil {
			return err
		}

	default:
		return fmt.Errorf("Cannot register backend; %T does not implement backend.IBackend", b)
	}

	return nil
}
