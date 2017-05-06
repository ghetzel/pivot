package dal

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/typeutil"
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
var DefaultIdentityFieldType Type = IntType

// Used by consumers Collection.NewInstance that wish to modify the instance
// before returning it
type InitializerFunc func(interface{}) interface{} // {}

type Instantiator interface {
	Constructor() interface{}
}

type Collection struct {
	Name                string  `json:"name"`
	Fields              []Field `json:"fields"`
	IdentityField       string  `json:"identity_field,omitempty"`
	IdentityFieldType   Type    `json:"identity_field_type,omitempty"`
	recordType          reflect.Type
	instanceInitializer InitializerFunc
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

func (self *Collection) SetRecordType(in interface{}) *Collection {
	inV := reflect.ValueOf(in)

	if inV.Kind() == reflect.Ptr {
		inV = inV.Elem()
	}

	inT := inV.Type()

	switch inT.Kind() {
	case reflect.Struct, reflect.Map:
		self.recordType = inT
	}

	return self
}

func (self *Collection) HasRecordType() bool {
	if self.recordType != nil {
		return true
	}

	return false
}

func (self *Collection) SetInitializer(init InitializerFunc) {
	self.instanceInitializer = init
}

func (self *Collection) NewInstance(initializers ...InitializerFunc) interface{} {
	if self.recordType == nil {
		panic("Cannot create instance without a registered type")
	}

	var instance interface{}
	var instanceV reflect.Value

	switch self.recordType.Kind() {
	case reflect.Map:
		instanceV = reflect.MakeMap(self.recordType)
		instance = instanceV.Interface()
	default:
		instance = reflect.New(self.recordType).Interface()
		instanceV = reflect.ValueOf(instance).Elem()
	}

	structFields, _ := getFieldsForStruct(instance)

	for _, field := range self.Fields {
		var zeroValue interface{}

		if field.DefaultValue == nil {
			zeroValue = field.GetTypeInstance()
		} else {
			zeroValue = field.GetDefaultValue()
		}

		zeroV := reflect.ValueOf(zeroValue)

		if zeroV.IsValid() {
			switch instanceV.Kind() {
			case reflect.Map:
				mapKeyT := instanceV.Type().Key()
				mapValueT := instanceV.Type().Elem()

				keyV := reflect.ValueOf(field.Name)

				if keyV.IsValid() {
					if !keyV.Type().AssignableTo(mapKeyT) {
						if keyV.Type().ConvertibleTo(mapKeyT) {
							keyV = keyV.Convert(mapKeyT)
						} else {
							continue
						}
					}

					if !zeroV.Type().AssignableTo(mapValueT) {
						if zeroV.Type().ConvertibleTo(mapValueT) {
							zeroV = zeroV.Convert(mapValueT)
						} else {
							continue
						}
					}

					instanceV.SetMapIndex(keyV, zeroV)
				}

			case reflect.Struct:
				if structFields != nil {
					if fieldDescr, ok := structFields[field.Name]; ok {
						fieldT := fieldDescr.ReflectField.Type()

						if !zeroV.Type().AssignableTo(fieldT) {
							if zeroV.Type().ConvertibleTo(fieldT) {
								zeroV = zeroV.Convert(fieldT)
							} else {
								continue
							}
						}

						fieldDescr.Field.Set(zeroV.Interface())
					}
				}
			}
		}
	}

	// apply schema-wide initializer if specified
	if self.instanceInitializer != nil {
		instance = self.instanceInitializer(instance)
	}

	// apply any call-time initializers
	for _, init := range initializers {
		instance = init(instance)
	}

	// apply the instance-specific initializer (if implemented)
	if init, ok := instance.(Instantiator); ok {
		instance = init.Constructor()
	}

	return instance
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

// Generates a Record instance from the given value based on this collection's schema.
func (self *Collection) MakeRecord(in interface{}) (*Record, error) {
	if err := validatePtrToStructType(in); err != nil {
		return nil, err
	}

	// if the argument is already a record, return it as-is
	if record, ok := in.(*Record); ok {
		// we're returning the record we were given, but first we need to validate and format it
		for key, value := range record.Fields {
			if field, ok := self.GetField(key); ok {
				if v, err := field.Format(value, PersistOperation); err == nil {
					if err := field.Validate(v); err == nil {
						record.Fields[key] = v
					} else {
						return nil, err
					}
				} else {
					return nil, err
				}
			} else {
				delete(record.Fields, key)
			}
		}

		return record, nil
	}

	// create the record we're going to populate
	record := NewRecord(nil)

	// get details for the fields present on the given input struct
	if fields, err := getFieldsForStruct(in); err == nil {
		// for each field descriptor...
		for tagName, fieldDescr := range fields {
			if fieldDescr.Field.IsExported() {
				value := fieldDescr.Field.Value()

				// set the ID field if this field is explicitly marked as the identity
				if fieldDescr.Identity && !fieldDescr.Field.IsZero() {
					record.ID = value
				} else {
					if collectionField, ok := self.GetField(tagName); ok {
						// validate and format value according to the collection field's rules
						if v, err := collectionField.Format(value, PersistOperation); err == nil {
							if err := collectionField.Validate(v); err == nil {
								value = v
							} else {
								return nil, err
							}
						} else {
							return nil, err
						}

						// if we're supposed to skip empty values, and this value is indeed empty, skip
						if fieldDescr.OmitEmpty && typeutil.IsZero(value) {
							continue
						}

						// set the value in the output record
						record.Set(tagName, value)
					}
				}
			}
		}

		// an identity column was not explicitly specified, so try to find the column that matches
		// our identity field name
		if record.ID == nil {
			for tagName, fieldDescr := range fields {
				if tagName == self.IdentityField {
					if fieldDescr.Field.IsExported() {
						// skip fields containing a zero value
						if !fieldDescr.Field.IsZero() {
							record.ID = fieldDescr.Field.Value()
							delete(record.Fields, tagName)
							break
						}
					}
				}
			}
		}

		// an ID still wasn't found, so try the field called "id"
		if record.ID == nil {
			if f, ok := fields[`id`]; ok {
				if !f.Field.IsZero() {
					record.ID = f.Field.Value()
					delete(record.Fields, `id`)
				}
			}
		}

		// an ID still wasn't found, so try the field called "ID"
		if record.ID == nil {
			if f, ok := fields[`ID`]; ok {
				if !f.Field.IsZero() {
					record.ID = f.Field.Value()
					delete(record.Fields, `ID`)
				}
			}
		}

		return record, nil
	} else {
		return nil, err
	}
}

func (self *Collection) Diff(actual *Collection) []SchemaDelta {
	differences := make([]SchemaDelta, 0)

	if self.Name != actual.Name {
		differences = append(differences, SchemaDelta{
			Type:       CollectionDelta,
			Issue:      CollectionNameIssue,
			Message:    `names do not match`,
			Collection: self.Name,
			Desired:    self.Name,
			Actual:     actual.Name,
		})
	}

	if self.IdentityField != actual.IdentityField {
		differences = append(
			differences,
			SchemaDelta{
				Type:       CollectionDelta,
				Issue:      CollectionKeyNameIssue,
				Message:    `does not match`,
				Collection: self.Name,
				Parameter:  `IdentityField`,
				Desired:    self.IdentityField,
				Actual:     actual.IdentityField,
			},
		)
	}

	if self.IdentityFieldType != actual.IdentityFieldType {
		differences = append(differences, SchemaDelta{
			Type:       CollectionDelta,
			Issue:      CollectionKeyTypeIssue,
			Message:    `does not match`,
			Collection: self.Name,
			Parameter:  `IdentityFieldType`,
			Desired:    self.IdentityFieldType,
			Actual:     actual.IdentityFieldType,
		})
	}

	for _, myField := range self.Fields {
		if theirField, ok := actual.GetField(myField.Name); ok {
			if diff := myField.Diff(&theirField); diff != nil {
				for i, _ := range diff {
					diff[i].Collection = self.Name
				}

				differences = append(differences, diff...)
			}
		} else {
			differences = append(differences, SchemaDelta{
				Type:       FieldDelta,
				Issue:      FieldMissingIssue,
				Message:    `is missing`,
				Collection: self.Name,
				Name:       myField.Name,
			})
		}
	}

	if len(differences) == 0 {
		return nil
	}

	return differences
}
