package dal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
)

var FieldNestingSeparator string = `.`

type Record struct {
	ID     interface{}            `json:"id"`
	Fields map[string]interface{} `json:"fields,omitempty"`
	Data   []byte                 `json:"data,omitempty"`
	Error  error                  `json:"error,omitempty"`
}

func NewRecord(id interface{}) *Record {
	return &Record{
		ID:     id,
		Fields: make(map[string]interface{}),
	}
}

func NewRecordErr(id interface{}, err error) *Record {
	return &Record{
		ID:     id,
		Fields: make(map[string]interface{}),
		Error:  err,
	}
}

func (self *Record) init() {
	if self.Fields == nil {
		self.Fields = make(map[string]interface{})
	}
}

func (self *Record) Keys(collection *Collection) []interface{} {
	values := sliceutil.Sliceify(self.ID)

	if collection != nil {
		for _, field := range collection.Fields {
			// if there are more keys than we currently have values...
			if collection.KeyCount() > len(values) {
				if field.Key {
					if value := self.Get(field.Name); value != nil {
						// append this key value
						values = append(values, value)
					}
				}
			} else {
				break
			}
		}
	}

	return values
}

func (self *Record) SetKeys(collection *Collection, op FieldOperation, keys ...interface{}) error {
	if collection != nil {
		if len(keys) > 0 {
			self.ID = collection.ConvertValue(collection.GetIdentityFieldName(), keys[0])
			i := 1

			for _, field := range collection.Fields {
				// if there are more keys than we currently have values...
				if i < len(keys) {
					if field.Key {
						if value, err := collection.ValueForField(field.Name, keys[i], op); err == nil {
							self.Set(field.Name, value)
							i += 1
						} else {
							return err
						}
					}
				} else {
					break
				}
			}
		}
	}

	return nil
}

func (self *Record) Copy(other *Record) {
	if other != nil {
		self.ID = other.ID
		self.Fields = other.Fields
		self.Data = other.Data
	}
}

func (self *Record) Get(key string, fallback ...interface{}) interface{} {
	self.init()

	if key == DefaultIdentityField {
		return self.ID
	} else if v, ok := self.Fields[key]; ok {
		return v
	} else {
		return self.GetNested(key, fallback...)
	}
}

func (self *Record) GetNested(key string, fallback ...interface{}) interface{} {
	self.init()

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

func (self *Record) GetString(key string, fallback ...string) string {
	if v := self.Get(key); v == nil {
		if len(fallback) > 0 {
			return fallback[0]
		} else {
			return ``
		}
	} else {
		return fmt.Sprintf("%v", v)
	}
}

func (self *Record) Set(key string, value interface{}) *Record {
	self.init()

	self.Fields[key] = value
	return self
}

func (self *Record) SetNested(key string, value interface{}) *Record {
	self.init()

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

func (self *Record) String() string {
	if data, err := json.Marshal(self); err == nil {
		return string(data)
	} else {
		return fmt.Sprintf("Record<%v + %d fields>", self.ID, len(self.Fields))
	}
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
	// special case for what is essentially copying another record into this one
	if record, ok := into.(*Record); ok {
		if collection != nil {
			collection.FillDefaults(self)
		}

		for key, value := range record.Fields {
			if collection != nil {
				if collectionField, ok := collection.GetField(key); ok {
					// use the field's type in the collection schema to convert the value
					if v, err := collectionField.ConvertValue(value); err == nil {
						value = v
					} else {
						log.Warningf("error populating field %q: %v", key, err)
						continue
					}

					// apply formatters to this value
					if v, err := collectionField.Format(value, RetrieveOperation); err == nil {
						value = v
					} else {
						log.Warningf("error formatting field %q: %v", key, err)
						continue
					}

					// this specifies that we should double-check the validity of the values coming in
					if collectionField.ValidateOnPopulate {
						// validate the value
						if err := collectionField.Validate(value); err != nil {
							return err
						}
					}
				}
			}

			self.Set(key, value)
		}

		self.ID = record.ID

		if collection != nil {
			if idI, err := collection.formatAndValidateId(self.ID, RetrieveOperation, self); err == nil {
				self.ID = idI
			} else {
				log.Warningf("error formatting ID: %v", err)
			}
		}
	} else {
		if err := validatePtrToStructType(into); err != nil {
			return err
		}

		// if the struct we got is a zero value, and we've been given a collection,
		// use it with NewInstance
		if collection != nil {
			if typeutil.IsZero(into) {
				into = collection.NewInstance()
			}
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
			// get field descriptors for the output struct
			if fields, err := getFieldsForStruct(into); err == nil {
				// for each value in the record's fields map...
				for key, value := range self.Fields {
					// only operate on fields that exist in the output struct
					if field, ok := fields[key]; ok {
						// only operate on exported output struct fields
						if field.Field.IsExported() {
							// skip the identity field, we handle that separately
							if field.Identity || field.Field.Name() == idFieldName {
								continue
							}

							// if a collection is specified, then use the corresponding field from that collection
							// to format the value first
							if collection != nil {
								if collectionField, ok := collection.GetField(key); ok {
									// use the field's type in the collection schema to convert the value
									if v, err := collectionField.ConvertValue(value); err == nil {
										value = v
									} else {
										return err
									}

									// apply formatters to this value
									if v, err := collectionField.Format(value, RetrieveOperation); err == nil {
										value = v
									} else {
										return err
									}

									// this specifies that we should double-check the validity of the values coming in
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
								if typeutil.IsZero(value) {
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

							// last-ditch effort to handle weird edge cases
							switch field.ReflectField.Type().String() {
							case `time.Time`, `*time.Time`:
								isPtr := strings.HasPrefix(field.ReflectField.Type().String(), `*`)

								if v, err := stringutil.ConvertToTime(value); err == nil {
									if isPtr {
										value = &v
									} else {
										value = v
									}
								} else if v, err := stringutil.ConvertToInteger(value); err == nil {
									var vT time.Time

									// guess at whether we're dealing with epoch seconds or nanoseconds
									if v <= 4294967296 {
										vT = time.Unix(v, 0)
									} else {
										vT = time.Unix(0, v)
									}

									if isPtr {
										value = &vT
									} else {
										value = vT
									}
								} else {
									return err
								}
							}

							if err := typeutil.SetValue(field.ReflectField, value); err != nil {
								log.Warningf("Failed to set field %s: %v", field.Field.Name(), err)
							}
						}
					}
				}
			} else {
				return err
			}

			// get the underlying field from the struct we're outputting to
			if _, ok := instanceStruct.FieldOk(idFieldName); ok {
				// if possible, format and validate the record ID first.
				// this lets us create (for example) random IDs
				if collection != nil {
					if idI, err := collection.formatAndValidateId(self.ID, RetrieveOperation, self); err == nil {
						self.ID = idI
					} else {
						return err
					}
				}

				if self.ID != nil {
					id := self.ID
					reflectField := reflect.ValueOf(into)

					if reflectField.Kind() == reflect.Ptr {
						reflectField = reflectField.Elem()
					}

					reflectField = reflectField.FieldByName(idFieldName)

					if err := typeutil.SetValue(reflectField, id); err != nil {
						return fmt.Errorf("Field '%s' is not settable: %v", idFieldName, err)
					}
				}
			}
		} else {
			return fmt.Errorf("Could not determine identity field name")
		}
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
