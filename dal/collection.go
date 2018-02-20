package dal

import (
	"fmt"
	"reflect"

	"github.com/ghetzel/go-stockutil/typeutil"
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
	Name                     string                  `json:"name"`
	IndexName                string                  `json:"index_name,omitempty"`
	IndexCompoundFields      []string                `json:"index_compound_fields,omitempty"`
	IndexCompoundFieldJoiner string                  `json:"index_compound_field_joiner,omitempty"`
	SkipIndexPersistence     bool                    `json:"skip_index_persistence,omitempty"`
	Fields                   []Field                 `json:"fields"`
	IdentityField            string                  `json:"identity_field,omitempty"`
	IdentityFieldType        Type                    `json:"identity_field_type,omitempty"`
	IdentityFieldFormatter   FieldFormatterFunc      `json:"-"`
	IdentityFieldValidator   FieldValidatorFunc      `json:"-"`
	PreSaveValidator         CollectionValidatorFunc `json:"-"`
	recordType               reflect.Type
	instanceInitializer      InitializerFunc
}

func NewCollection(name string) *Collection {
	return &Collection{
		Name:                   name,
		Fields:                 make([]Field, 0),
		IdentityField:          DefaultIdentityField,
		IdentityFieldType:      DefaultIdentityFieldType,
		IdentityFieldValidator: ValidateNotEmpty,
	}
}

func (self *Collection) GetIndexName() string {
	if self.IndexName != `` {
		return self.IndexName
	}

	return self.Name
}

func (self *Collection) GetAggregatorName() string {
	if self.IndexName != `` {
		return self.IndexName
	}

	return self.Name
}

func (self *Collection) SetIdentity(name string, idtype Type, formatter FieldFormatterFunc, validator FieldValidatorFunc) *Collection {
	if name != `` {
		self.IdentityField = name
	}

	self.IdentityFieldType = idtype

	if formatter != nil {
		self.IdentityFieldFormatter = formatter
	}

	if validator != nil {
		self.IdentityFieldValidator = validator
	}

	return self
}

func (self *Collection) AddFields(fields ...Field) *Collection {
	self.Fields = append(self.Fields, fields...)
	return self
}

// Copies certain collection and field properties from the definition object into this collection
// instance.  This is useful for collections that are created by parsing the schema as it exists on
// the remote datastore, which will have some but not all of the information we need to work with the
// data.  Definition collections are the authoritative source for things like what the default value
// should be, and which validators and formatters apply to a given field.
//
// This function converts this instance into a Collection definition by copying the relevant values
// from given definition.
//
func (self *Collection) ApplyDefinition(definition *Collection) error {
	if definition != nil {
		if v := definition.IdentityField; v != `` {
			self.IdentityField = v
		}

		if v := definition.IdentityFieldType; v != `` {
			self.IdentityFieldType = v
		}

		if fn := definition.IdentityFieldFormatter; fn != nil {
			self.IdentityFieldFormatter = fn
		}

		if fn := definition.IdentityFieldValidator; fn != nil {
			self.IdentityFieldValidator = fn
		}

		for i, field := range self.Fields {
			if defField, ok := definition.GetField(field.Name); ok {
				if field.Description == `` {
					self.Fields[i].Description = defField.Description
				}

				if field.Length == 0 && defField.Length != 0 {
					self.Fields[i].Length = defField.Length
				}

				if field.Precision == 0 && defField.Precision != 0 {
					self.Fields[i].Precision = defField.Precision
				}

				// unconditionally pull these over as they are either client-only fields or we know better
				// than the database on this one
				self.Fields[i].Required = defField.Required
				self.Fields[i].Type = defField.Type
				self.Fields[i].KeyType = defField.KeyType
				self.Fields[i].Subtype = defField.Subtype
				self.Fields[i].DefaultValue = defField.DefaultValue
				self.Fields[i].ValidateOnPopulate = defField.ValidateOnPopulate
				self.Fields[i].Validator = defField.Validator
				self.Fields[i].Formatter = defField.Formatter
			} else {
				return fmt.Errorf("Definition is missing field %q", field.Name)
			}
		}
	}

	return nil
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

func (self *Collection) FillDefaults(record *Record) {
	for _, field := range self.Fields {
		if field.DefaultValue != nil {
			if typeutil.IsZero(record.Get(field.Name)) {
				record.Set(field.Name, field.GetDefaultValue())
			}
		}
	}
}

func (self *Collection) GetField(name string) (Field, bool) {
	for _, field := range self.Fields {
		if field.Name == name {
			return field, true
		}
	}

	if name == self.IdentityField || (self.IdentityField == `` && name == DefaultIdentityField) {
		return Field{
			Name:     self.IdentityField,
			Type:     self.IdentityFieldType,
			Identity: true,
			Key:      true,
			Required: true,
		}, true
	}

	return Field{}, false
}

func (self *Collection) IsIdentityField(name string) bool {
	if field, ok := self.GetField(name); ok {
		return field.Identity
	}

	return false
}

func (self *Collection) IsKeyField(name string) bool {
	if field, ok := self.GetField(name); ok {
		return (field.Key && !field.Identity)
	}

	return false
}

func (self *Collection) GetFirstNonIdentityKeyField() (Field, bool) {
	for _, field := range self.Fields {
		if field.Key && !field.Identity {
			return field, true
		}
	}

	return Field{}, false
}

func (self *Collection) ConvertValue(name string, value interface{}) interface{} {
	if field, ok := self.GetField(name); ok {
		if v, err := field.ConvertValue(value); err == nil {
			return v
		}
	}

	return value
}

func (self *Collection) formatAndValidateId(id interface{}, op FieldOperation, record *Record) (interface{}, error) {
	// if specified, apply a formatter to the ID
	if self.IdentityFieldFormatter != nil {
		// NOTE: because we want the option to generate IDs based on the values of other record fields,
		//       we pass the whole record into IdentityFieldFormatters
		if idI, err := self.IdentityFieldFormatter(record, op); err == nil {
			id = idI
		} else {
			return id, err
		}
	}

	// if given, validate the ID value
	if self.IdentityFieldValidator != nil {
		if err := self.IdentityFieldValidator(id); err != nil {
			return id, err
		}
	}

	return id, nil
}

// Generates a Record instance from the given value based on this collection's schema.
func (self *Collection) MakeRecord(in interface{}) (*Record, error) {
	var idFieldName string

	if err := validatePtrToStructType(in); err != nil {
		return nil, err
	}

	// if the argument is already a record, return it as-is
	if record, ok := in.(*Record); ok {
		self.FillDefaults(record)

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

		// validate ID value
		if idI, err := self.formatAndValidateId(record.ID, PersistOperation, record); err == nil {
			record.ID = idI
		} else {
			return nil, err
		}

		// validate whole record (if specified)
		if err := self.ValidateRecord(record, PersistOperation); err != nil {
			return nil, err
		}

		return record, nil
	}

	// create the record we're going to populate
	record := NewRecord(nil)

	// populate it with default values
	self.FillDefaults(record)

	// get details for the fields present on the given input struct
	if fields, err := getFieldsForStruct(in); err == nil {
		// for each field descriptor...
		for tagName, fieldDescr := range fields {
			if fieldDescr.Field.IsExported() {
				value := fieldDescr.Field.Value()

				// set the ID field if this field is explicitly marked as the identity
				if fieldDescr.Identity && !typeutil.IsZero(fieldDescr.Field) {
					idFieldName = tagName
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

						// make sure the corresponding value in the input struct matches
						fieldDescr.Field.Set(value)
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
						if !typeutil.IsZero(fieldDescr.Field) {
							idFieldName = tagName
							record.ID = fieldDescr.Field.Value()
							delete(record.Fields, tagName)
							break
						}
					}
				}
			}
		}

		// an ID still wasn't found, so try the field called "id"
		for _, fieldName := range []string{`id`, `ID`, `Id`} {
			if record.ID == nil {
				if f, ok := fields[fieldName]; ok {
					if !f.Field.IsZero() {
						idFieldName = fieldName
						record.ID = f.Field.Value()
						delete(record.Fields, fieldName)
						break
					}
				}
			}
		}

		if idI, err := self.formatAndValidateId(record.ID, PersistOperation, record); err == nil {
			record.ID = idI

			// make sure the corresponding ID in the input struct matches
			if fieldDescr, ok := fields[idFieldName]; ok {
				if err := fieldDescr.Field.Set(idI); err != nil {
					return nil, fmt.Errorf("failed to writeback value to %q: %v", idFieldName, err)
				}
			}
		} else {
			return nil, err
		}

		// validate whole record (if specified)
		if err := self.ValidateRecord(record, PersistOperation); err != nil {
			return nil, err
		}

		return record, nil
	} else {
		return nil, err
	}
}

func (self *Collection) ValidateRecord(record *Record, op FieldOperation) error {
	switch op {
	case PersistOperation:
		// validate whole record (if specified)
		if self.PreSaveValidator != nil {
			if err := self.PreSaveValidator(record); err != nil {
				return err
			}
		}
	}

	return nil
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
