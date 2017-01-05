package dal

import (
	"fmt"
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

// errors
var CollectionNotFound = fmt.Errorf("Collection not found")

type Collection struct {
	Dataset       *Dataset               `json:"-"`
	Name          string                 `json:"name"`
	Fields        []Field                `json:"fields"`
	IdentityField string                 `json:"identity_field,omitempty"`
	Properties    map[string]interface{} `json:"properties"`
}

func NewCollection(name string) *Collection {
	return &Collection{
		Name:          name,
		Fields:        make([]Field, 0),
		Properties:    make(map[string]interface{}),
		IdentityField: DefaultIdentityField,
	}
}

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

func (self *Collection) VerifyEqual(other Collection) error {
	if other.Name != self.Name {
		return fmt.Errorf("Collection names do not match; expected: '%s', got: '%s'", self.Name, other.Name)
	}

	for myKey, myValue := range self.Properties {
		if otherValue, ok := other.Properties[myKey]; ok {
			if otherValue != myValue {
				return fmt.Errorf("Collection property '%s' values differ", myKey)
			}
		} else {
			return fmt.Errorf("Collection property '%s' is missing", myKey)
		}
	}

	for otherKey, otherValue := range other.Properties {
		if myValue, ok := self.Properties[otherKey]; ok {
			if myValue != otherValue {
				return fmt.Errorf("Collection property '%s' values differ", otherKey)
			}
		} else {
			return fmt.Errorf("Collection property '%s' is missing", otherKey)
		}
	}

	if len(self.Fields) != len(other.Fields) {
		return fmt.Errorf("Collection field counts differ; expected: %d, got: %d", len(self.Fields), len(other.Fields))
	}

	for i, myField := range self.Fields {
		var otherField Field

		if self.Dataset.FieldsUnordered {
			if f, ok := other.GetField(myField.Name); ok {
				otherField = f
			} else {
				return fmt.Errorf("Collection field '%s' is missing", myField.Name)
			}
		} else {
			otherField = other.Fields[i]
		}

		if err := otherField.VerifyEqual(self.Dataset, myField); err != nil {
			return err
		}
	}

	return nil
}
