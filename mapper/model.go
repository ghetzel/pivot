package mapper

// The mapper package provides a simplified, high-level interface for
// interacting with database objects.

import (
	"fmt"
	"reflect"

	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type ResultFunc func(ptrToInstance interface{}, err error) // {}

type Mapper interface {
	NewInstance(inits ...dal.InitializerFunc) interface{}
	GetBackend() backends.Backend
	Migrate() error
	Drop() error
	Exists(id interface{}) bool
	Create(from interface{}) error
	Get(id interface{}, into interface{}) error
	Update(from interface{}) error
	CreateOrUpdate(id interface{}, from interface{}) error
	Delete(ids ...interface{}) error
	Find(flt interface{}, into interface{}) error
	FindFunc(flt interface{}, destZeroValue interface{}, resultFn ResultFunc) error
	All(into interface{}) error
	Each(destZeroValue interface{}, resultFn ResultFunc) error
	List(fields []string) (map[string][]interface{}, error)
	ListWithFilter(fields []string, flt interface{}) (map[string][]interface{}, error)
	Sum(field string, flt interface{}) (float64, error)
	Count(flt interface{}) (uint64, error)
	Minimum(field string, flt interface{}) (float64, error)
	Maximum(field string, flt interface{}) (float64, error)
	Average(field string, flt interface{}) (float64, error)
	GroupBy(fields []string, aggregates []filter.Aggregate, flt interface{}) (*dal.RecordSet, error)
}

type Model struct {
	Mapper
	db         backends.Backend
	collection *dal.Collection
}

func NewModel(db backends.Backend, collection *dal.Collection) *Model {
	model := new(Model)

	model.db = db
	model.collection = collection

	if model.collection.Fields == nil {
		model.collection.Fields = make([]dal.Field, 0)
	}

	if v := collection.IdentityField; v == `` {
		model.collection.IdentityField = dal.DefaultIdentityField
	} else {
		model.collection.IdentityField = v
	}

	if v := collection.IdentityFieldType; v == `` {
		model.collection.IdentityFieldType = dal.DefaultIdentityFieldType
	} else {
		model.collection.IdentityFieldType = v
	}

	db.RegisterCollection(collection)

	return model
}

func (self *Model) NewInstance(inits ...dal.InitializerFunc) interface{} {
	if self.collection == nil {
		panic("Collection-aware instance creation is not supported on anonymous Models")
	}

	return self.collection.NewInstance(inits...)
}

func (self *Model) GetBackend() backends.Backend {
	return self.db
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
	} else if err != nil {
		return err
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

	// overlay the definition onto whatever the backend came back with
	actualCollection.ApplyDefinition(self.collection)

	return nil
}

func (self *Model) Drop() error {
	return self.db.DeleteCollection(self.collection.Name)
}

// Creates and saves a new instance of the model from the given struct or dal.Record.
//
func (self *Model) Create(from interface{}) error {
	if record, err := self.collection.MakeRecord(from); err == nil {
		return self.db.Insert(self.collection.Name, dal.NewRecordSet(record))
	} else {
		return err
	}
}

// Retrieves an instance of the model identified by the given ID and populates the value pointed to
// by the into parameter.  Structs and dal.Record instances can be populated.
//
func (self *Model) Get(id interface{}, into interface{}) error {
	if record, err := self.db.Retrieve(self.collection.Name, id); err == nil {
		return record.Populate(into, self.collection)
	} else {
		return err
	}
}

// Tests whether a record exists for the given ID.
//
func (self *Model) Exists(id interface{}) bool {
	return self.db.Exists(self.collection.Name, id)
}

// Updates and saves an existing instance of the model from the given struct or dal.Record.
//
func (self *Model) Update(from interface{}) error {
	if record, err := self.collection.MakeRecord(from); err == nil {
		return self.db.Update(self.collection.Name, dal.NewRecordSet(record))
	} else {
		return err
	}
}

// Creates or updates an instance of the model depending on whether it exists or not.
//
func (self *Model) CreateOrUpdate(id interface{}, from interface{}) error {
	if id == nil || !self.Exists(id) {
		return self.Create(from)
	} else {
		return self.Update(from)
	}
}

// Delete instances of the model identified by the given IDs
//
func (self *Model) Delete(ids ...interface{}) error {
	return self.db.Delete(self.collection.Name, ids...)
}

// Perform a query for instances of the model that match the given filter.Filter.
// Results will be returned in the slice or array pointed to by the into parameter, or
// if into points to a dal.RecordSet, the RecordSet resulting from the query will be returned
// as-is.
//
func (self *Model) Find(flt interface{}, into interface{}) error {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if search := self.db.WithSearch(self.collection.Name, f); search != nil {
			// perform query
			if recordset, err := search.Query(self.collection.Name, f); err == nil {
				return self.populateOutputParameter(f, recordset, into)
			} else {
				return err
			}
		} else {
			return fmt.Errorf("backend %T does not support searching", self.db)
		}
	} else {
		return err
	}
}

// Perform a query for instances of the model that match the given filter.Filter.
// The given callback function will be called once per result.
//
func (self *Model) FindFunc(flt interface{}, destZeroValue interface{}, resultFn ResultFunc) error {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.Limit = 0
		f.IdentityField = self.collection.IdentityField

		if search := self.db.WithSearch(self.collection.Name, f); search != nil {
			_, err := search.Query(self.collection.Name, f, func(record *dal.Record, err error, _ backends.IndexPage) error {
				if err == nil {
					if _, ok := destZeroValue.(*dal.Record); ok {
						resultFn(record, nil)
					} else if _, ok := destZeroValue.(dal.Record); ok {
						resultFn(*record, nil)
					} else {
						into := reflect.New(reflect.TypeOf(destZeroValue)).Interface()

						// populate that type with data from this record
						if err := record.Populate(into, self.collection); err == nil {
							resultFn(into, nil)
						} else {
							return err
						}
					}
				} else {
					resultFn(nil, err)
				}

				return nil
			})

			return err
		} else {
			return fmt.Errorf("backend %T does not support searching", self.db)
		}
	} else {
		return err
	}
}

func (self *Model) All(into interface{}) error {
	return self.Find(filter.All, into)
}

func (self *Model) Each(destZeroValue interface{}, resultFn ResultFunc) error {
	return self.FindFunc(filter.All, destZeroValue, resultFn)
}

func (self *Model) List(fields []string) (map[string][]interface{}, error) {
	return self.ListWithFilter(fields, filter.All)
}

func (self *Model) ListWithFilter(fields []string, flt interface{}) (map[string][]interface{}, error) {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if search := self.db.WithSearch(self.collection.Name, f); search != nil {
			return search.ListValues(self.collection.Name, fields, f)
		} else {
			return nil, fmt.Errorf("backend %T does not support searching", self.db)
		}
	} else {
		return nil, err
	}
}

func (self *Model) Sum(field string, flt interface{}) (float64, error) {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection.Name); agg != nil {
			return agg.Sum(self.collection.Name, field, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) Count(flt interface{}) (uint64, error) {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection.Name); agg != nil {
			return agg.Count(self.collection.Name, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) Minimum(field string, flt interface{}) (float64, error) {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection.Name); agg != nil {
			return agg.Minimum(self.collection.Name, field, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) Maximum(field string, flt interface{}) (float64, error) {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection.Name); agg != nil {
			return agg.Maximum(self.collection.Name, field, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) Average(field string, flt interface{}) (float64, error) {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection.Name); agg != nil {
			return agg.Average(self.collection.Name, field, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) GroupBy(fields []string, aggregates []filter.Aggregate, flt interface{}) (*dal.RecordSet, error) {
	if f, err := self.filterFromInterface(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection.Name); agg != nil {
			return agg.GroupBy(self.collection.Name, fields, aggregates, f)
		} else {
			return nil, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return nil, err
	}
}

func (self *Model) filterFromInterface(in interface{}) (filter.Filter, error) {
	if f, ok := in.(filter.Filter); ok {
		return f, nil
	} else if f, ok := in.(*filter.Filter); ok {
		return *f, nil
	} else if fMap, ok := in.(map[string]interface{}); ok {
		return filter.FromMap(fMap)
	} else if fStr, ok := in.(string); ok {
		return filter.Parse(fStr)
	} else {
		return filter.Null, fmt.Errorf("Expected filter.Filter, map[string]interface{}, or string; got: %T", in)
	}
}

func (self *Model) populateOutputParameter(f filter.Filter, recordset *dal.RecordSet, into interface{}) error {
	vInto := reflect.ValueOf(into)

	// get value pointed to if we were given a pointer
	if vInto.Kind() == reflect.Ptr {
		vInto = vInto.Elem()
	} else {
		return fmt.Errorf("Output argument must be a pointer")
	}

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
			if len(f.Fields) > 0 {
				for k, _ := range record.Fields {
					if !sliceutil.ContainsString(f.Fields, k) {
						delete(record.Fields, k)
					}
				}
			}

			// make a new zero-valued instance of the slice type
			elem := reflect.New(sliceType)

			// if we have a registered collection, use its
			if self.collection != nil && self.collection.HasRecordType() {
				elem = reflect.ValueOf(self.collection.NewInstance())
			}

			// populate that type with data from this record
			if err := record.Populate(elem.Interface(), self.collection); err == nil {
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
}
