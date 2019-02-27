package dal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
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

func (self *Record) Copy(other *Record, schema ...*Collection) error {
	var collection *Collection

	if len(schema) > 0 && schema[0] != nil {
		collection = schema[0]
	}

	if other != nil {
		self.Data = other.Data

		if collection != nil {
			collection.FillDefaults(self)
		}

		for key, value := range other.Fields {
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

		self.ID = other.ID

		if collection != nil {
			if idI, err := collection.formatAndValidateId(self.ID, RetrieveOperation, self); err == nil {
				self.ID = idI
			} else {
				log.Warningf("error formatting ID: %v", err)
			}
		}
	}

	return nil
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
func (self *Record) Populate(into interface{}, collection *Collection) error {
	// special case for what is essentially copying another record into this one
	if record, ok := into.(*Record); ok {
		return self.Copy(record, collection)
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

		var idFieldName string
		var fallbackIdFieldName string

		// if a collection is specified, set the fallback identity field name to what the collection
		// knows the ID field is called.  This is used for input structs that don't explicitly tag
		// a field with the ",identity" option
		if collection != nil {
			idFieldName = collection.GetIdentityFieldName()
		}

		// get the name of the identity field from the given struct
		if id, err := GetIdentityFieldName(into, fallbackIdFieldName); err == nil && id != `` {
			idFieldName = id
		} else if err != nil {
			return err
		} else {
			return fmt.Errorf("Could not determine identity field name")
		}

		if data, err := self.toMap(collection, idFieldName); err == nil {
			return maputil.TaggedStructFromMap(data, into, RecordStructTag)
		} else {
			return err
		}
	}
}

func (self *Record) toMap(collection *Collection, idFieldName string) (map[string]interface{}, error) {
	data := make(map[string]interface{})

	// populate defaults
	if collection != nil {
		for _, field := range collection.Fields {
			data[field.Name] = field.GetDefaultValue()
		}
	}

	// get identity field name
	if idFieldName == `` {
		if collection != nil {
			idFieldName = collection.GetIdentityFieldName()
		} else {
			idFieldName = DefaultIdentityField
		}
	}

	// set values
	data[idFieldName] = self.ID

	for k, v := range self.Fields {
		// if the field we're setting already exists (i.e.: has a default value), that value
		// isn't a zero value, but the incoming one IS a zero value, skip.
		if existing, ok := data[k]; ok {
			// TODO: using straight Zero Value detection is insufficient for bool fields, and possibly time.Time.  Look into this.
			if !typeutil.IsZero(existing) && typeutil.IsZero(v) {
				continue
			}
		}

		data[k] = v
	}

	if collection != nil {
		// format and validate values (including identity)
		for key, value := range data {
			if key == idFieldName {
				if idI, err := collection.formatAndValidateId(value, RetrieveOperation, self); err == nil {
					value = idI
				} else {
					return nil, err
				}
			} else if collectionField, ok := collection.GetField(key); ok {
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
						return nil, err
					}
				}

			}

			data[key] = value
		}

		// cull values that aren't fields in the collection
		for k, _ := range data {
			if k == idFieldName {
				continue
			} else if _, ok := collection.GetField(k); !ok {
				delete(data, k)
			}
		}
	}

	return data, nil
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
