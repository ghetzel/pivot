package dal

import (
	"fmt"
	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/maputil"
	"reflect"
	"strings"
)

var FieldNestingSeparator string = `.`

type Record struct {
	ID     interface{}            `json:"id"`
	Fields map[string]interface{} `json:"fields,omitempty"`
	Data   []byte                 `json:"data,omitempty"`
}

func NewRecord(id interface{}) *Record {
	return &Record{
		ID:     id,
		Fields: make(map[string]interface{}),
	}
}

func (self *Record) Get(key string, fallback ...interface{}) interface{} {
	if v, ok := self.Fields[key]; ok {
		return v
	} else {
		return self.GetNested(key, fallback...)
	}
}

func (self *Record) GetNested(key string, fallback ...interface{}) interface{} {
	var fb interface{}

	if len(fallback) > 0 {
		fb = fallback[0]
	}

	return maputil.DeepGet(
		self.Fields,
		strings.Split(key, FieldNestingSeparator),
		fb,
	)
}

func (self *Record) Set(key string, value interface{}) *Record {
	self.Fields[key] = value
	return self
}

func (self *Record) SetNested(key string, value interface{}) *Record {
	parts := strings.Split(key, FieldNestingSeparator)
	maputil.DeepSet(self.Fields, parts, value)
	return self
}

func (self *Record) SetFields(values map[string]interface{}) *Record {
	for k, v := range values {
		self.Set(k, v)
	}

	return self
}

func (self *Record) SetData(data []byte) *Record {
	self.Data = data
	return self
}

func (self *Record) Append(key string, value ...interface{}) *Record {
	return self.Set(key, self.appendValue(key, value...))
}

func (self *Record) AppendNested(key string, value ...interface{}) *Record {
	return self.SetNested(key, self.appendValue(key, value...))
}

// Populates a given struct with with the values in this record.
//
func (self *Record) Populate(into interface{}, collection *Collection) error {
	if err := validatePtrToStructType(into); err != nil {
		return err
	}

	instanceStruct := structs.New(into)
	var idFieldName string
	var fallbackIdFieldName string

	// if a collection is specified, set the fallback identity field name to what the collection
	// knows the ID field is called.  This is used for input structs that don't explicitly tag
	// a field with the ",identity" option
	if collection != nil {
		if id := collection.IdentityField; id != `` {
			fallbackIdFieldName = id
		}
	}

	// get the name of the identity field from the given struct
	if id, err := GetIdentityFieldName(into, fallbackIdFieldName); err == nil {
		idFieldName = id
	} else {
		return err
	}

	if idFieldName != `` {
		// get the underlying field from the struct we're outputting to
		if idField, ok := instanceStruct.FieldOk(idFieldName); ok {
			if self.ID != nil {
				id := self.ID

				// we need to use reflect directly because structs Field.Set() involves
				// a type check that's too restrictive for us here
				reflectField := reflect.ValueOf(into)

				if reflectField.Kind() == reflect.Ptr {
					reflectField = reflectField.Elem()
				}

				reflectField = reflectField.FieldByName(idFieldName)

				fType := reflect.TypeOf(idField.Value())
				vValue := reflect.ValueOf(id)

				if fType != nil {
					// convert the value to the field's type if necessary
					if !vValue.Type().AssignableTo(fType) {
						if vValue.Type().ConvertibleTo(fType) {
							vValue = vValue.Convert(fType)
						}
					}
				}

				// set (via reflect) is we can
				if vValue.Type().AssignableTo(reflectField.Type()) {
					reflectField.Set(vValue)
				} else {
					return fmt.Errorf("Field '%s' is not settable", idFieldName)
				}
			}
		}

		// get field descriptors for the output struct
		if fields, err := getFieldsForStruct(into); err == nil {
			// for each value in the record's fields map...
			for key, value := range self.Fields {
				// only operate on fields that exist in the output struct
				if field, ok := fields[key]; ok {
					// only operate on exported output struct fields
					if field.Field.IsExported() {
						// skip the identity field, we already took care of this
						if field.Identity || field.Field.Name() == idFieldName {
							continue
						}

						// if a collection is specified, then use the corresponding field from that collection
						// to format the value first
						if collection != nil {
							if collectionField, ok := collection.GetField(key); ok {
								if v, err := collectionField.Format(value, RetrieveOperation); err == nil {
									value = v
								} else {
									return err
								}

								if collectionField.ValidateOnPopulate {
									// validate the value
									if err := collectionField.Validate(value); err != nil {
										return err
									}
								}
							} else {
								// because we were given a collection, we know whether we should actually
								// work with this field or not
								continue
							}
						}

						// skip values that are that type's zero value if OmitEmpty is set
						if field.OmitEmpty {
							if value == nil || value == reflect.Zero(reflect.TypeOf(value)).Interface() {
								continue
							}
						}

						// get the underlying type of the field
						fType := reflect.TypeOf(field.Field.Value())
						vValue := reflect.ValueOf(value)

						// convert the value to the field's type if necessary
						if fType != nil {
							if vValue.IsValid() {
								if !vValue.Type().AssignableTo(fType) {
									if vValue.Type().ConvertibleTo(fType) {
										vValue = vValue.Convert(fType)
										value = vValue.Interface()
									}
								}
							} else {
								value = reflect.Zero(fType).Interface()
							}
						}

						// set (via reflect) if we can
						if vValue.IsValid() {
							if vValue.Type().AssignableTo(field.ReflectField.Type()) {
								field.ReflectField.Set(vValue)
							} else {
								return fmt.Errorf("Field '%s' is not settable", field.Field.Name())
							}
						} else {
							if err := field.Field.Set(value); err != nil {
								return err
							}
						}
					}
				}
			}
		} else {
			return err
		}
	} else {
		return fmt.Errorf("Could not determine identity field name")
	}

	return nil
}

func (self *Record) appendValue(key string, value ...interface{}) []interface{} {
	newValue := make([]interface{}, 0)

	if v := self.Get(key); v != nil {
		valueV := reflect.ValueOf(v)

		switch valueV.Type().Kind() {
		case reflect.Array, reflect.Slice:
			for i := 0; i < valueV.Len(); i++ {
				newValue = append(newValue, valueV.Index(i).Interface())
			}
		default:
			newValue = append(newValue, v)
		}
	}

	return append(newValue, value...)
}
