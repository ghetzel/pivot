package dal

import (
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

func (self *Record) Populate(instance interface{}) error {
	if err := validatePtrToStructType(instance); err != nil {
		return err
	}

	instanceStruct := structs.New(instance)

	// get the name of the identity field from the given struct
	if idFieldName, err := GetIdentityFieldName(instance); err == nil {
		// get the underlying field from the struct we're outputting to
		if idField, ok := instanceStruct.FieldOk(idFieldName); ok {
			id := self.ID
			fType := reflect.TypeOf(idField.Value())
			vValue := reflect.ValueOf(id)

			// convert the value to the field's type if necessary
			if !vValue.Type().AssignableTo(fType) {
				if vValue.Type().ConvertibleTo(fType) {
					id = vValue.Convert(fType).Interface()
				}
			}

			// get the ID value
			if err := idField.Set(id); err != nil {
				return err
			}
		}

		// get field descriptors for the output struct
		if fields, err := getFieldsForStruct(instanceStruct); err == nil {
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

						// skip values that are that type's zero value if OmitEmpty is set
						if field.OmitEmpty && value == reflect.Zero(reflect.TypeOf(value)).Interface() {
							continue
						}

						// get the underling type of the field
						fType := reflect.TypeOf(field.Field.Value())
						vValue := reflect.ValueOf(value)

						// convert the value to the field's type if necessary
						if !vValue.Type().AssignableTo(fType) {
							if vValue.Type().ConvertibleTo(fType) {
								value = vValue.Convert(fType).Interface()
							}
						}

						// set the value
						if err := field.Field.Set(value); err != nil {
							return err
						}
					}
				}
			}
		} else {
			return err
		}
	} else {
		return err
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
