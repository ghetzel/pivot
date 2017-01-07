package dal

import (
	"fmt"
	"reflect"
)

type CollectionAction int

const (
	SchemaVerify CollectionAction = iota
	SchemaCreate
	SchemaExpand
	SchemaRemove
	SchemaEnforce
)

var DefaultIdentityField = `id`
var DefaultIdentityFieldType = `int`

type CollectionOptions struct {
	FieldsUnordered bool `json:"fields_unordered,omitempty"`
}

type Collection struct {
	Name              string             `json:"name"`
	Fields            []Field            `json:"fields"`
	IdentityField     string             `json:"identity_field,omitempty"`
	IdentityFieldType string             `json:"identity_field_type,omitempty"`
	Options           *CollectionOptions `json:"options,omitempty"`
	recordType        reflect.Type
}

func NewCollection(name string) *Collection {
	return &Collection{
		Name:              name,
		Fields:            make([]Field, 0),
		IdentityField:     DefaultIdentityField,
		IdentityFieldType: DefaultIdentityFieldType,
	}
}

func (self *Collection) AddFields(fields ...Field) *Collection {
	self.Fields = append(self.Fields, fields...)
	return self
}

// func (self *Collection) SetRecordType(in interface{}) *Collection {
// 	inT := reflect.TypeOf(in)

// 	switch inT.Kind() {
// 	case reflect.Struct, reflect.Map:
// 		self.recordType = inT
// 	default:
// 		fallbackType := make(map[string]interface{})
// 		self.recordType = reflect.TypeOf(fallbackType)
// 	}

// 	return self
// }

func (self *Collection) GetField(name string) (Field, bool) {
	for _, field := range self.Fields {
		if field.Name == name {
			return field, true
		}
	}

	return Field{}, false
}

func (self *Collection) ConvertValue(name string, value interface{}) (interface{}, error) {
	if field, ok := self.GetField(name); ok {
		return field.ConvertValue(value)
	} else {
		return nil, fmt.Errorf("Unknown field '%s'", name)
	}
}
