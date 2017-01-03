package dal

import (
	"fmt"
)

type FieldProperties struct {
	Identity     bool        `json:"identity,omitempty"`
	Key          bool        `json:"key,omitempty"`
	Required     bool        `json:"required,omitempty"`
	Unique       bool        `json:"unique,omitempty"`
	DefaultValue interface{} `json:"default,omitempty"`
	NativeType   string      `json:"native_type,omitempty"`
}

type Field struct {
	Name       string           `json:"name"`
	Type       string           `json:"type"`
	Length     int              `json:"length,omitempty"`
	Properties *FieldProperties `json:"properties,omitempty"`
}

func (self *Field) VerifyEqual(dataset *Dataset, other Field) error {
	if other.Name != self.Name {
		return fmt.Errorf("Field names do not match; expected: '%s', got: '%s'", self.Name, other.Name)
	}

	if other.Type != self.Type {
		return fmt.Errorf("Field types do not match; expected: '%s', got: '%s'", self.Type, other.Type)
	}

	if !dataset.SkipFieldLength {
		if other.Length != self.Length {
			return fmt.Errorf("Field lengths do not match; expected: %d, got: %d", self.Length, other.Length)
		}
	}
	return nil
}
