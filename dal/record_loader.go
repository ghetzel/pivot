package dal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/structutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/util"
)

var RecordStructTag = util.RecordStructTag
var DefaultStructIdentityFieldName = `ID`

type fieldDescription struct {
	OriginalName string
	RecordKey    string
	Identity     bool
	OmitEmpty    bool
	FieldValue   reflect.Value
	FieldType    reflect.Type
	DataValue    interface{}
}

func (self *fieldDescription) Set(value interface{}) error {
	if self.FieldValue.IsValid() {
		if self.FieldValue.CanSet() {
			return typeutil.SetValue(self.FieldValue, value)
		} else {
			return fmt.Errorf("cannot set field %q: field is unsettable", self.OriginalName)
		}
	} else {
		return fmt.Errorf("cannot set field %q: no value available", self.OriginalName)
	}
}

type Model interface{}

func structFieldToDesc(field *reflect.StructField) *fieldDescription {
	desc := new(fieldDescription)
	desc.OriginalName = field.Name
	desc.RecordKey = field.Name

	if tag := field.Tag.Get(RecordStructTag); tag != `` {
		tag = strings.TrimSpace(tag)
		key, rest := stringutil.SplitPair(tag, `,`)
		options := strings.Split(rest, `,`)

		if key != `` {
			desc.RecordKey = key
		}

		for _, opt := range options {
			switch opt {
			case `identity`:
				desc.Identity = true
			case `omitempty`:
				desc.OmitEmpty = true
			}
		}
	}

	return desc
}

func getIdentityFieldName(in interface{}, collection *Collection) string {
	candidates := make([]string, 0)

	if err := structutil.FieldsFunc(in, func(field *reflect.StructField, value reflect.Value) error {
		desc := structFieldToDesc(field)

		if desc.Identity {
			candidates = append(candidates, field.Name)
		} else if collection != nil && field.Name == collection.GetIdentityFieldName() {
			candidates = append(candidates, field.Name)
		} else {
			switch field.Name {
			case `id`, `ID`, `Id`:
				candidates = append(candidates, field.Name)
			}
		}

		return nil
	}); err == nil {
		if len(candidates) > 0 {
			return candidates[0]
		}
	}

	return ``
}

// Retrieves the struct field name and key name that represents the identity field for a given struct.
func getIdentityFieldNameFromStruct(instance interface{}, fallbackIdentityFieldName string) (string, string, error) {
	if err := validatePtrToStructType(instance); err != nil {
		return ``, ``, err
	}

	var structFieldName string
	var dbFieldName string
	var fieldNames = make(map[string]bool)

	// find a field with an ",identity" tag and get its value
	var fn structutil.StructFieldFunc

	fn = func(field *reflect.StructField, v reflect.Value) error {
		if field.Anonymous && v.CanInterface() {
			var substruct interface{} = v.Interface()

			if v.CanAddr() {
				substruct = v.Addr().Interface()
			}

			return structutil.FieldsFunc(substruct, fn)
		} else {
			fieldNames[field.Name] = true

			if tag := field.Tag.Get(RecordStructTag); tag != `` {
				if v := strings.Split(tag, `,`); sliceutil.ContainsString(v[1:], `identity`) {
					structFieldName = field.Name

					if v[0] != `` {
						dbFieldName = v[0]
					}

					return structutil.StopIterating
				}
			}

			return nil
		}
	}

	structutil.FieldsFunc(instance, fn)

	if structFieldName != `` {
		if dbFieldName != `` {
			return structFieldName, dbFieldName, nil
		} else {
			return structFieldName, structFieldName, nil
		}
	}

	if fallbackIdentityFieldName == `` {
		fallbackIdentityFieldName = DefaultStructIdentityFieldName
	}

	if _, ok := fieldNames[fallbackIdentityFieldName]; ok {
		return fallbackIdentityFieldName, fallbackIdentityFieldName, nil
	} else if _, ok := fieldNames[DefaultStructIdentityFieldName]; ok {
		return DefaultStructIdentityFieldName, DefaultStructIdentityFieldName, nil
	}

	return ``, ``, fmt.Errorf("No identity field could be found for type %T", instance)
}

func validatePtrToStructType(instance interface{}) error {
	vInstance := reflect.ValueOf(instance)

	if vInstance.IsValid() {
		if vInstance.Kind() == reflect.Ptr {
			vInstance = vInstance.Elem()
		} else {
			return fmt.Errorf("Can only operate on pointer to struct, got %T", instance)
		}

		if vInstance.Kind() == reflect.Struct {
			return nil
		} else {
			return fmt.Errorf("Can only operate on pointer to struct, got %T", instance)
		}
	} else {
		return fmt.Errorf("invalid value %T", instance)
	}
}

// Retrieve details about a specific field in the given struct. This function parses the `pivot`
// tag details into discrete values, extracts the concrete value of the field, and returns the
// reflected Type and Value of the field.
func getFieldForStruct(instance interface{}, key string) (*fieldDescription, error) {
	var desc *fieldDescription

	// iterate over all exported struct fields
	if err := structutil.FieldsFunc(instance, func(field *reflect.StructField, value reflect.Value) error {
		desc = structFieldToDesc(field)

		// either the field name OR the name specified in the "pivot" tag will match
		if field.Name == key || desc.RecordKey == key {
			desc.FieldValue = value
			desc.FieldType = value.Type()

			if value.CanInterface() {
				desc.DataValue = value.Interface()
			}

			return structutil.StopIterating
		} else {
			return nil
		}
	}); err == nil {
		if desc == nil {
			return nil, fmt.Errorf("No such field %q", key)
		} else {
			return desc, nil
		}
	} else {
		return nil, err
	}
}
