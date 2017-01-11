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

	if idField, err := GetIdentityFieldName(instance); err == nil {
		if idField, ok := instanceStruct.FieldOk(idField); ok {
			idField.Set(self.ID)
		}

		if fields, err := getFieldsForStruct(instanceStruct); err == nil {
			for key, value := range self.Fields {
				if field, ok := fields[key]; ok {
					// skip the identity field
					if field.Identity {
						continue
					}

					// skip values that are that type's zero value if OmitEmpty is set
					if field.OmitEmpty && value == reflect.Zero(reflect.TypeOf(value)) {
						continue
					}

					// set the value
					field.Field.Set(value)
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
