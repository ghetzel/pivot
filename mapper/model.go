package mapper

import (
	"fmt"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"reflect"
)

type Mapper interface {
	Migrate() error
	Exists(id interface{}) bool
	Create(from interface{}) error
	Get(id interface{}, into interface{}) error
	Update(from interface{}) error
	CreateOrUpdate(id interface{}, from interface{}) error
	Delete(ids ...interface{}) error
	Find(f filter.Filter, into interface{}) error
	All(into interface{}) error
}

type Model struct {
	Mapper
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
	var actualCollection *dal.Collection

	// create the collection if it doesn't exist
	if c, err := self.db.GetCollection(self.collection.Name); dal.IsCollectionNotFoundErr(err) {
		if err := self.db.CreateCollection(self.collection); err == nil {
			if c, err := self.db.GetCollection(self.collection.Name); err == nil {
				actualCollection = c
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		actualCollection = c
	}

	if diffs := self.collection.Diff(actualCollection); diffs != nil {
		msg := fmt.Sprintf("Actual schema for collection '%s' differs from desired schema:\n", self.collection.Name)

		for _, err := range diffs {
			msg += fmt.Sprintf("  %v\n", err)
		}

		return fmt.Errorf(msg)
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

func (self *Model) Exists(id interface{}) bool {
	return self.db.Exists(self.collection.Name, id)
}

func (self *Model) Update(from interface{}) error {
	if record, err := self.collection.MakeRecord(from); err == nil {
		return self.db.Update(self.collection.Name, dal.NewRecordSet(record))
	} else {
		return err
	}
}

func (self *Model) CreateOrUpdate(id interface{}, from interface{}) error {
	if id == nil || !self.Exists(id) {
		return self.Create(from)
	} else {
		return self.Update(from)
	}
}

func (self *Model) Delete(ids ...interface{}) error {
	return self.db.Delete(self.collection.Name, ids...)
}

func (self *Model) Find(f filter.Filter, into interface{}) error {
	if search := self.db.WithSearch(); search != nil {
		vInto := reflect.ValueOf(into)

		// get value pointed to if we were given a pointer
		if vInto.Kind() == reflect.Ptr {
			vInto = vInto.Elem()
		} else {
			return fmt.Errorf("Output argument must be a pointer")
		}

		// perform query
		if recordset, err := search.Query(self.collection.Name, f); err == nil {
			// we're going to fill arrays or slices
			switch vInto.Type().Kind() {
			case reflect.Array, reflect.Slice:
				indirectResult := true

				// get the type of the underlying slice element
				sliceType := vInto.Type().Elem()

				// get the type pointed to
				if sliceType.Kind() == reflect.Ptr {
					sliceType = sliceType.Elem()
					indirectResult = false
				}

				// for each resulting record...
				for _, record := range recordset.Records {
					// make a new zero-valued instance of the slice type
					elem := reflect.New(sliceType)

					// populate that type with data from this record
					if err := record.Populate(elem.Interface()); err == nil {
						// if the slice elements are pointers, we can append the pointer we just created as-is
						// otherwise, we need to indirect the value and append a copy

						if indirectResult {
							vInto.Set(reflect.Append(vInto, reflect.Indirect(elem)))
						} else {
							vInto.Set(reflect.Append(vInto, elem))
						}
					} else {
						return err
					}
				}

				return nil
			case reflect.Struct:
				if rs, ok := into.(*dal.RecordSet); ok {
					*rs = *recordset
					return nil
				}

				fallthrough
			default:
				return fmt.Errorf("model can only populate results into slice or array, got %T", into)
			}
		} else {
			return err
		}
	} else {
		return fmt.Errorf("backend %T does not support searching", self.db)
	}
}

func (self *Model) All(into interface{}) error {
	return self.Find(filter.All, into)
}
