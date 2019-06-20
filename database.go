package pivot

import (
	"fmt"

	"github.com/ghetzel/pivot/v3/backends"
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

type db struct {
	backends.Backend
	models map[string]Model
}

func newdb(backend backends.Backend) *db {
	return &db{
		Backend: backend,
		models:  make(map[string]Model),
	}
}

func (self *db) GetBackend() Backend {
	return self.Backend
}

func (self *db) SetBackend(backend Backend) {
	self.Backend = backend
}

func (self *db) AttachCollection(collection *Collection) Model {
	model := mapper.NewModel(self, collection)

	self.models[collection.Name] = model
	return model
}

func (self *db) Migrate() error {
	for name, model := range self.models {
		if err := model.Migrate(); err != nil {
			return fmt.Errorf("failed to migrate %s: %v", name, err)
		}
	}

	return nil
}

func (self *db) Models() (models []Model) {
	for _, model := range self.models {
		models = append(models, model)
	}

	return
}

func (self *db) ApplySchemata(fileOrDirPath string) error {
	return ApplySchemata(fileOrDirPath, self)
}

func (self *db) LoadFixtures(fileOrDirPath string) error {
	return LoadFixtures(fileOrDirPath, self)
}
