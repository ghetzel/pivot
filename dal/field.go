package dal

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/sliceutil"
)

type Field struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Length     int                    `json:"length,omitempty"`
	Properties map[string]interface{} `json:"properties,omitempty"`
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

	for myKey, myValue := range self.Properties {
		if otherValue, ok := other.Properties[myKey]; ok {
			if otherValue != myValue {
				return fmt.Errorf("Field '%s': property '%s' values differ", self.Name, myKey)
			}
		} else {
			if sliceutil.ContainsString(dataset.MandatoryFieldProperties, myKey) {
				return fmt.Errorf("Field '%s': property '%s' is missing", self.Name, myKey)
			}
		}
	}

	for otherKey, otherValue := range other.Properties {
		if myValue, ok := self.Properties[otherKey]; ok {
			if myValue != otherValue {
				return fmt.Errorf("Field '%s': property '%s' values differ", self.Name, otherKey)
			}
		} else {
			if sliceutil.ContainsString(dataset.MandatoryFieldProperties, otherKey) {
				return fmt.Errorf("Field '%s': property '%s' is missing", self.Name, otherKey)
			}
		}
	}

	return nil
}
