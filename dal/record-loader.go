package dal

import (
	"fmt"
	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"reflect"
	"strings"
)

var RecordStructTag = `pivot`

type fieldDescription struct {
	Field     *structs.Field
	Identity  bool
	OmitEmpty bool
}

type Model interface{}

// Extract the collection name and ID value from a given struct.
//
func GetCollectionAndIdentity(instance interface{}) (string, interface{}, error) {
	var collection string
	var id interface{}

	if err := validatePtrToStructType(instance); err != nil {
		return ``, nil, err
	}

	s := structs.New(instance)

	// get the collection name or use the struct's type name
	if field, ok := s.FieldOk(`Model`); ok {
		v := strings.Split(field.Tag(RecordStructTag), `,`)
		collection = v[0]
	}

	if collection == `` {
		v := strings.Split(fmt.Sprintf("%T", instance), `.`)
		collection = v[len(v)-1]
	}

	// get the value from the identity field
	if idFieldName, err := GetIdentityFieldName(instance); err == nil {
		if field, ok := s.FieldOk(idFieldName); ok {
			id = field.Value()
		}
	} else {
		return ``, nil, err
	}

	return collection, id, nil
}

func GetIdentityFieldName(instance interface{}) (string, error) {
	if err := validatePtrToStructType(instance); err != nil {
		return ``, err
	}

	s := structs.New(instance)

	// find a field with an ",identity" tag and get its value
	for _, field := range s.Fields() {
		if tag := field.Tag(RecordStructTag); tag != `` {
			v := strings.Split(tag, `,`)

			if sliceutil.ContainsString(v[1:], `identity`) {
				return field.Name(), nil
			}
		}
	}

	if _, ok := s.FieldOk(`ID`); ok {
		return `ID`, nil
	}

	return ``, fmt.Errorf("No identity field could be found for type %T", instance)
}

func validatePtrToStructType(instance interface{}) error {
	vInstance := reflect.ValueOf(instance)

	if vInstance.IsValid() {
		if vInstance.Kind() == reflect.Ptr {
			vInstance = vInstance.Elem()
		}

		if vInstance.Kind() == reflect.Struct {
			return nil
		}
	} else {
		return fmt.Errorf("invalid value %T", instance)
	}

	return fmt.Errorf("Can only operate on pointer to struct, got %T", instance)
}

func getFieldsForStruct(instance *structs.Struct) (map[string]fieldDescription, error) {
	fields := make(map[string]fieldDescription)
	identitySet := false

	for _, field := range instance.Fields() {
		var identity, omitEmpty bool

		name := field.Name()

		if tag := field.Tag(RecordStructTag); tag != `` {
			v := strings.Split(tag, `,`)

			if v[0] != `` {
				name = v[0]
			}

			if len(v) > 1 {
				identity = sliceutil.ContainsString(v[1:], `identity`)
				omitEmpty = sliceutil.ContainsString(v[1:], `omitempty`)
			}
		}

		if !identitySet && identity {
			identitySet = true
		}

		fields[name] = fieldDescription{
			Field:     field,
			Identity:  identity,
			OmitEmpty: omitEmpty,
		}
	}

	return fields, nil
}
