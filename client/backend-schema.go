package pivot

import (
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/dal"
)

func (self *Backend) CreateCollection(definition dal.Collection) error {
	path := self.GetPath(`schema`, definition.Name, `create`)

	if _, err := self.Client.Call(`PUT`, path, &definition); err == nil {
		return self.Refresh()
	} else {
		return err
	}
}

func (self *Backend) DeleteCollection(name string) error {
	if _, err := self.Client.Call(`DELETE`, self.GetPath(`schema`, name), nil); err == nil {
		return self.Refresh()
	} else {
		return err
	}
}

func (self *Backend) GetCollection(name string) (dal.Collection, error) {
	collection := self.GetDataset().MakeCollection(name)

	if response, err := self.Client.Call(`GET`, self.GetPath(`schema`, name), nil); err == nil {
		if err := maputil.StructFromMap(response.Payload, &collection); err == nil {
			return collection, nil
		} else {
			return collection, err
		}
	} else {
		return collection, err
	}
}
