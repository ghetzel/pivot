package mapper

// The mapper package provides a simplified, high-level interface for
// interacting with database objects.

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type Mapper = backends.Mapper
type ResultFunc = backends.ResultFunc

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

	// tell the backend about the collection
	db.RegisterCollection(collection)

	// ...and tell the collection about the backend
	collection.SetBackend(db)

	return model
}

func (self *Model) String() string {
	return self.collection.Name
}

func (self *Model) GetBackend() backends.Backend {
	return self.db
}

func (self *Model) GetCollection() *dal.Collection {
	return self.collection
}

func (self *Model) Migrate() error {
	var actualCollection *dal.Collection

	// create the collection if it doesn't exist
	if c, err := self.db.GetCollection(self.collection.Name); dal.ShouldCreateCollection(self.collection, err) {
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

	// TODO: uncommenting this produces an infinite loop; investigate
	// if migratable, ok := self.db.(dal.Migratable); ok {
	// 	return migratable.Migrate()
	// }

	if diffs := self.collection.Diff(actualCollection); diffs != nil {
		merr := fmt.Errorf("Actual schema for collection '%s' differs from desired schema", self.collection.Name)

		for _, diff := range diffs {
			merr = log.AppendError(merr, fmt.Errorf("- %s", diff.String()))
		}

		return merr
	} else {
		log.Debugf("Collection %q is up-to-date with the given schema", self.collection.Name)
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
	if record, err := self.collection.StructToRecord(from); err == nil {
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
	if record, err := self.collection.StructToRecord(from); err == nil {
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

// Delete all records matching the given query.
func (self *Model) DeleteQuery(flt interface{}) error {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if search := self.db.WithSearch(self.collection, f); search != nil {
			return search.DeleteQuery(self.collection, f)
		} else {
			return fmt.Errorf("backend %T does not support searching", self.db)
		}
	} else {
		return err
	}
}

// Perform a query for instances of the model that match the given filter.Filter.
// Results will be returned in the slice or array pointed to by the into parameter, or
// if into points to a dal.RecordSet, the RecordSet resulting from the query will be returned
// as-is.
//
func (self *Model) Find(flt interface{}, into interface{}) error {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if search := self.db.WithSearch(self.collection, f); search != nil {
			// perform query
			if recordset, err := search.Query(self.collection, f); err == nil {
				return self.populateOutputParameter(f, recordset, into)
			} else {
				return fmt.Errorf("Cannot perform query: %v", err)
			}
		} else {
			return fmt.Errorf("backend %T does not support searching", self.db)
		}
	} else {
		return fmt.Errorf("Cannot generate filter: %v", err)
	}
}

// Perform a query for instances of the model that match the given filter.Filter.
// The given callback function will be called once per result.
//
func (self *Model) FindFunc(flt interface{}, destZeroValue interface{}, resultFn ResultFunc) error {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if search := self.db.WithSearch(self.collection, f); search != nil {
			_, err := search.Query(self.collection, f, func(record *dal.Record, err error, _ backends.IndexPage) error {
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
	return self.Find(filter.All(), into)
}

func (self *Model) Each(destZeroValue interface{}, resultFn ResultFunc) error {
	return self.FindFunc(filter.All(), destZeroValue, resultFn)
}

func (self *Model) List(fields []string) (map[string][]interface{}, error) {
	return self.ListWithFilter(fields, filter.All())
}

func (self *Model) ListWithFilter(fields []string, flt interface{}) (map[string][]interface{}, error) {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if search := self.db.WithSearch(self.collection, f); search != nil {
			if fieldValues, err := search.ListValues(self.collection, fields, f); err == nil {
				for field, values := range fieldValues {
					sort.Slice(values, func(i int, j int) bool {
						return typeutil.String(values[i]) < typeutil.String(values[j])
					})

					fieldValues[field] = values
				}

				return fieldValues, nil
			} else {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("backend %T does not support searching", self.db)
		}
	} else {
		return nil, err
	}
}

func (self *Model) Sum(field string, flt interface{}) (float64, error) {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection); agg != nil {
			return agg.Sum(self.collection, field, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) Count(flt interface{}) (uint64, error) {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection); agg != nil {
			return agg.Count(self.collection, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) Minimum(field string, flt interface{}) (float64, error) {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection); agg != nil {
			return agg.Minimum(self.collection, field, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) Maximum(field string, flt interface{}) (float64, error) {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection); agg != nil {
			return agg.Maximum(self.collection, field, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) Average(field string, flt interface{}) (float64, error) {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		if agg := self.db.WithAggregator(self.collection); agg != nil {
			return agg.Average(self.collection, field, f)
		} else {
			return 0, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return 0, err
	}
}

func (self *Model) GroupBy(fields []string, aggregates []filter.Aggregate, flt interface{}) (*dal.RecordSet, error) {
	if f, err := filter.Parse(flt); err == nil {
		f.IdentityField = self.collection.IdentityField

		for i, agg := range aggregates {
			if agg.Field == `` {
				aggregates[i].Field = f.IdentityField
			}
		}

		if agg := self.db.WithAggregator(self.collection); agg != nil {
			return agg.GroupBy(self.collection, fields, aggregates, f)
		} else {
			return nil, fmt.Errorf("backend %T does not support aggregation", self.db)
		}
	} else {
		return nil, err
	}
}

func (self *Model) populateOutputParameter(f *filter.Filter, recordset *dal.RecordSet, into interface{}) error {
	// for each resulting record...
	for _, record := range recordset.Records {
		if len(f.Fields) > 0 {
			for k, _ := range record.Fields {
				if !sliceutil.ContainsString(f.Fields, k) {
					delete(record.Fields, k)
				}
			}
		}
	}

	return recordset.PopulateFromRecords(into, self.collection)
}
