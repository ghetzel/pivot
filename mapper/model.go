package mapper

import (
	"fmt"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"reflect"
)

type Model struct {
	db         backends.Backend
	collection *dal.Collection
}

func NewModel(db backends.Backend, collection dal.Collection) *Model {
	model := new(Model)

	model.db = db
	model.collection = dal.NewCollection(collection.Name)
	model.collection.Fields = collection.Fields

	if v := collection.IdentityField; v != `` {
		model.collection.IdentityField = v
	}

	if v := collection.IdentityFieldType; v != `` {
		model.collection.IdentityFieldType = v
	}

	return model
}

func (self *Model) Migrate() error {
	// create the collection if it doesn't exist
	if _, err := self.db.GetCollection(self.collection.Name); dal.IsCollectionNotFoundErr(err) {
		if err := self.db.CreateCollection(self.collection); err != nil {
			return err
		}
	}

	return nil
}

func (self *Model) Create(from interface{}) error {
	if record, err := self.collection.MakeRecord(from); err == nil {
		return self.db.Insert(self.collection.Name, dal.NewRecordSet(record))
	} else {
		return err
	}
}

func (self *Model) Get(id interface{}, into interface{}) error {
	if record, err := self.db.Retrieve(self.collection.Name, id); err == nil {
		return record.Populate(into)
	} else {
		return err
	}
}

func (self *Model) Update(from interface{}) error {
	if record, err := self.collection.MakeRecord(from); err == nil {
		return self.db.Update(self.collection.Name, dal.NewRecordSet(record))
	} else {
		return err
	}
}

func (self *Model) Delete(ids ...interface{}) error {
	return self.db.Delete(self.collection.Name, ids...)
}

func (self *Model) Find(f filter.Filter, into interface{}) error {
	if search := self.db.WithSearch(); search != nil {
		vInto := reflect.ValueOf(into)

		switch vInto.Type().Kind() {
		case reflect.Array, reflect.Slice:
			intoElemType := vInto.Type().Elem()

			if recordset, err := search.Query(self.collection.Name, f); err == nil {
				for _, record := range recordset.Records {
					item := reflect.New(intoElemType).Interface()

					if err := record.Populate(item); err == nil {
						vInto.Set(reflect.Append(vInto, reflect.ValueOf(item)))
					} else {
						return err
					}
				}

				return nil
			} else {
				return err
			}
		default:
			return fmt.Errorf("model can only populate results into slice or array, got %T", self, into)
		}
	} else {
		return fmt.Errorf("backend %T does not support searching", self.db)
	}
}
