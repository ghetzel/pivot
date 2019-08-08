package pivot

import (
	"fmt"

	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/mapper"
)

type DB interface {
	backends.Backend
	AttachCollection(*Collection) Model
	Migrate() error
	Models() []Model
	ApplySchemata(fileOrDirPath string) error
	LoadFixtures(fileOrDirPath string) error
	GetBackend() Backend
	SetBackend(Backend)
}

type schemaModel struct {
	Collection *dal.Collection
	Model      Model
}

func (self *schemaModel) String() string {
	if self.Collection != nil {
		return self.Collection.Name
	} else {
		return ``
	}
}

type db struct {
	backends.Backend
	models []*schemaModel
}

func newdb(backend backends.Backend) *db {
	return &db{
		Backend: backend,
		models:  make([]*schemaModel, 0),
	}
}

func (self *db) GetBackend() Backend {
	return self.Backend
}

func (self *db) SetBackend(backend Backend) {
	self.Backend = backend
}

func (self *db) AttachCollection(collection *Collection) Model {
	if collection == nil {
		panic("cannot attach nil Collection")
	}

	for _, sm := range self.models {
		if sm.String() == collection.Name {
			panic(fmt.Sprintf("Collection %q is already registered", collection.Name))
		}
	}

	sm := &schemaModel{
		Collection: collection,
		Model:      mapper.NewModel(self, collection),
	}

	self.models = append(self.models, sm)
	return sm.Model
}

func (self *db) Migrate() error {
	for _, sm := range self.models {
		if err := sm.Model.Migrate(); err != nil {
			return fmt.Errorf("failed to migrate %v: %v", sm, err)
		}
	}

	return nil
}

func (self *db) Models() (models []Model) {
	for _, sm := range self.models {
		models = append(models, sm.Model)
	}

	return
}

func (self *db) ApplySchemata(fileOrDirPath string) error {
	return ApplySchemata(fileOrDirPath, self)
}

func (self *db) LoadFixtures(fileOrDirPath string) error {
	return LoadFixtures(fileOrDirPath, self)
}
