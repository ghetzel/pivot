package dal

import (
	"fmt"
	"reflect"
)

type Constraint struct {
	// Represents the name (or array of names) of the local field the constraint is being applied to.
	On interface{} `json:"on"`

	// The remote collection the constraint applies to.
	Collection string `json:"collection"`

	// The remote field (or fields) in the remote collection the constraint applies to.
	Field interface{} `json:"field"`

	// Provides backend-specific additional options for the constraint.
	Options string `json:"options,omitempty"`

	// Specifies the local field that related records will be put into.  Defaults to the field specified in On.
	Into string `json:"into,omitempty"`

	// Whether to omit this constraint when determining embedded collections.
	NoEmbed bool `json:"noembed,omitempty"`
}

func (self Constraint) Validate() error {
	if self.On == `` {
		return fmt.Errorf("invalid constraint missing local field")
	}

	if self.Collection == `` {
		return fmt.Errorf("invalid constraint missing remote collection name")
	}

	if self.Field == `` {
		return fmt.Errorf("invalid constraint missing remote field")
	}

	return nil
}

func (self Constraint) Equal(other *Constraint) bool {
	if self.On == other.On {
		if self.Collection == other.Collection {
			if reflect.DeepEqual(self.Field, other.Field) {
				return true
			}
		}
	}

	return false
}
