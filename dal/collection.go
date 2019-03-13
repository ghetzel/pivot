package dal

import (
	"fmt"
	"reflect"
	"time"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/sliceutil"
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
	// The name of the collection
	Name string `json:"name"`

	// The name of the associated external inverted index used to query this collection.
	IndexName string `json:"index_name,omitempty"`

	// Lists the field names that make up a composite key on this Collection that should be joined
	// together when determining index record IDs.  Often it is the case that external indices do
	// not support composite keys the way databases do, so this allows Collections with composite
	// keys to be indexed in those systems by joining several values together to form a unique key.
	IndexCompoundFields []string `json:"index_compound_fields,omitempty"`

	// The string used to join and split ID values that go into / come out of external indices.
	IndexCompoundFieldJoiner string `json:"index_compound_field_joiner,omitempty"`

	// Disable automatically dual-writing modified records into the external index.
	SkipIndexPersistence bool `json:"skip_index_persistence,omitempty"`

	// The fields that belong to this collection (all except the primary key/identity field/first
	// field in a composite key)
	Fields []Field `json:"fields"`

	// The name of the identity field for this Collection.  Defaults to "id".
	IdentityField string `json:"identity_field,omitempty"`

	// The datatype of the identity field.  Defaults to integer.
	IdentityFieldType Type `json:"identity_field_type,omitempty"`

	// Used to store the location of the identity field in the source database.
	IdentityFieldIndex int `json:"identity_field_index"`

	// Specifies how fields in this Collection relate to records from other collections.  This is
	// a partial implementation of a relational model, specifically capturing one-to-one or
	// one-to-many relationships.  The definitions here will retrieve the assocated records from
	// another, and those values will replace the value that is actually in this Collection's field.
	EmbeddedCollections []Relationship `json:"embed,omitempty"`

	// Specifies which fields can be seen when records are from relationships defined on other
	// Collections.  This can be used to restrict the exposure) of sensitive data in this Collection
	// be being an embedded field in another Collection.
	ExportedFields []string `json:"export,omitempty"`

	// Specify whether missing related fields generate an error when retrieving a record.
	AllowMissingEmbeddedRecords bool `json:"allow_missing_embedded_records"`

	// A read-only count of the number of records in this Collection
	TotalRecords int64 `json:"total_records,omitempty"`

	// Whether the value of TotalRecords represents an exact (authoritative) count or an
	// approximate count.
	TotalRecordsExact bool `json:"total_records_exact,omitempty"`

	// The name of a field containing an absolute datetime after which expired records should be
	// deleted from this Collection.
	TimeToLiveField string `json:"time_to_live_field"`

	// A function that modifies the identity key value before any operation.  Operates the same as
	// a Field Formatter function.
	IdentityFieldFormatter FieldFormatterFunc `json:"-"`

	// A function that validates the value of an identity key before create and update operations.
	// Operates the same as a Field Validator function.
	IdentityFieldValidator FieldValidatorFunc `json:"-"`

	// Allow backends to store internal information about the backing datasource for this collection.
	SourceURI string `json:"-"`

	// If specified, this function receives a copy of the populated record before create and update
	// operations, allowing for a last-chance validation of the record as a whole.  Use a pre-save
	// validator when validation requires checking multiple fields at once.
	PreSaveValidator CollectionValidatorFunc `json:"-"`

	recordType          reflect.Type
	instanceInitializer InitializerFunc
}

// Create a new colllection definition with no fields.
func NewCollection(name string) *Collection {
	return &Collection{
		Name:                   name,
		Fields:                 make([]Field, 0),
		IdentityField:          DefaultIdentityField,
		IdentityFieldType:      DefaultIdentityFieldType,
		IdentityFieldValidator: ValidateNotEmpty,
	}
}

// Return the duration until the TimeToLiveField in given record expires within the current collection.
// Collections with an empty TimeToLiveField, or records with a missing or zero-valued TimeToLiveField
// will return 0.  If the record has already expired, the returned duration will be a negative number.
func (self *Collection) TTL(record *Record) time.Duration {
	if self.TimeToLiveField != `` {
		if value := record.Get(self.TimeToLiveField); !typeutil.IsZero(value) {
			if expireAt := typeutil.V(value).Time(); !expireAt.IsZero() {
				return expireAt.Sub(time.Now())
			}
		}
	}

	return 0
}

// Expired records are those whose TTL duration is non-zero and negative.
func (self *Collection) IsExpired(record *Record) bool {
	if self.TTL(record) < 0 {
		return true
	} else {
		return false
	}
}

// Get the canonical name of the external index name.
func (self *Collection) GetIndexName() string {
	if self.IndexName != `` {
		return self.IndexName
	}

	return self.Name
}

// Get the canonical name of the dataset in an external aggregator service.
func (self *Collection) GetAggregatorName() string {
	if self.IndexName != `` {
		return self.IndexName
	}

	return self.Name
}

// Configure the identity field of a collection in a single function call.
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

// Append a field definition to this collection.
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

		if zeroV := reflect.ValueOf(zeroValue); zeroV.IsValid() {
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
						typeutil.SetValue(fieldDescr.ReflectField, zeroValue)
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

// Populate a given Record with the default values (if any) of all fields in the Collection.
func (self *Collection) FillDefaults(record *Record) {
	for _, field := range self.Fields {
		if field.DefaultValue != nil {
			if typeutil.IsZero(record.Get(field.Name)) {
				record.Set(field.Name, field.GetDefaultValue())
			}
		}
	}
}

// Retrieve a single field by name.  The second return value will be false if the field does not
// exist.
func (self *Collection) GetField(name string) (Field, bool) {
	if name == self.GetIdentityFieldName() {
		return Field{
			Name:     name,
			Type:     self.IdentityFieldType,
			Index:    self.IdentityFieldIndex,
			Identity: true,
			Key:      true,
			Required: true,
		}, true
	} else {
		for _, field := range self.Fields {
			if field.Name == name {
				return field, true
			}
		}
	}

	return Field{}, false
}

// Retrieve a single field by its index value. The second return value will be false if a field
// at that index does not exist.
func (self *Collection) GetFieldByIndex(index int) (Field, bool) {
	if index == self.IdentityFieldIndex {
		return Field{
			Name:     self.GetIdentityFieldName(),
			Type:     self.IdentityFieldType,
			Index:    self.IdentityFieldIndex,
			Identity: true,
			Key:      true,
			Required: true,
		}, true
	} else {
		for _, field := range self.Fields {
			if field.Index == index {
				return field, true
			}
		}
	}

	return Field{}, false
}

// Get the canonical name of the primary identity field.
func (self *Collection) GetIdentityFieldName() string {
	if self.IdentityField == `` {
		return DefaultIdentityField
	} else {
		return self.IdentityField
	}
}

// TODO: what is this?
func (self *Collection) IsIdentityField(name string) bool {
	if field, ok := self.GetField(name); ok {
		return field.Identity
	}

	return false
}

// Return whether a given field name is a key on this Collection.
func (self *Collection) IsKeyField(name string) bool {
	if field, ok := self.GetField(name); ok {
		return (field.Key && !field.Identity)
	}

	return false
}

// Retrieve all of the fields that comprise the primary key for this Collection.  This will always include the identity
// field at a minimum.
func (self *Collection) KeyFields() []Field {
	keys := []Field{
		Field{
			Name:     self.GetIdentityFieldName(),
			Type:     self.IdentityFieldType,
			Identity: true,
			Key:      true,
			Required: true,
		},
	}

	// append additional key fields
	for _, field := range self.Fields {
		if field.Key {
			keys = append(keys, field)
		}
	}

	return keys
}

// Return the number of keys on that uniquely identify a single record in this Collection.
func (self *Collection) KeyCount() int {
	return len(self.KeyFields())
}

// Retrieve the first non-indentity key field, sometimes referred to as the "range", "sort", or "cluster" key.
func (self *Collection) GetFirstNonIdentityKeyField() (Field, bool) {
	for _, field := range self.Fields {
		if field.Key && !field.Identity {
			return field, true
		}
	}

	return Field{}, false
}

// Convert a given value according to the data type of a specific named field.
func (self *Collection) ConvertValue(name string, value interface{}) interface{} {
	if field, ok := self.GetField(name); ok {
		if v, err := field.ConvertValue(value); err == nil {
			return v
		}
	}

	return value
}

// Convert a given value into one that that can go into the backend database (for create/update operations), or that
// should be returned to the user (for retrieval operations) in accordance with the named field's data type and
// formatters.  Invalid values (determined by Validators and the Required option in the Field) will return an error.
func (self *Collection) ValueForField(name string, value interface{}, op FieldOperation) (interface{}, error) {
	var formatter FieldFormatterFunc
	var validator FieldValidatorFunc

	if name == self.GetIdentityFieldName() {
		formatter = self.IdentityFieldFormatter
		validator = self.IdentityFieldValidator
		value = self.ConvertValue(name, value)
	} else if field, ok := self.GetField(name); ok {
		if v, err := field.ConvertValue(value); err == nil {
			formatter = field.Formatter
			validator = field.Validator

			// fmt.Printf("%v: value %T(%v) becomes %T(%v)\n", name, value, value, v, v)
			value = v
		} else {
			return nil, err
		}
	} else {
		return nil, FieldNotFound
	}

	if formatter != nil {
		if v, err := formatter(value, op); err == nil {
			value = v
		} else {
			return nil, err
		}
	}

	if validator != nil {
		if err := validator(value); err != nil {
			return nil, err
		}
	}

	return value, nil
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
func (self *Collection) MakeRecord(in interface{}, ops ...FieldOperation) (*Record, error) {
	var idFieldName string
	var op FieldOperation

	if len(ops) > 0 {
		op = ops[0]
	} else {
		op = PersistOperation
	}

	if err := validatePtrToStructType(in); err != nil {
		return nil, err
	}

	// if the argument is already a record, return it as-is
	if record, ok := in.(*Record); ok {
		self.FillDefaults(record)

		// we're returning the record we were given, but first we need to validate and format it
		for key, value := range record.Fields {
			if v, err := self.ValueForField(key, value, op); err == nil {
				record.Set(key, v)
			} else if IsFieldNotFoundErr(err) {
				delete(record.Fields, key)
			} else {
				return nil, err
			}
		}

		// validate ID value
		if idI, err := self.formatAndValidateId(record.ID, op, record); err == nil {
			record.ID = idI
		} else {
			return nil, err
		}

		// validate whole record (if specified)
		if err := self.ValidateRecord(record, op); err != nil {
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
				} else if finalValue, err := self.ValueForField(tagName, value, op); err == nil {
					// if we're supposed to skip empty values, and this value is indeed empty, skip
					if fieldDescr.OmitEmpty && typeutil.IsZero(finalValue) {
						continue
					}

					// set the value in the output record
					record.Set(tagName, finalValue)

					// make sure the corresponding value in the input struct matches
					typeutil.SetValue(fieldDescr.ReflectField, finalValue)
				}
			}
		}

		// an identity column was not explicitly specified, so try to find the column that matches
		// our identity field name
		if record.ID == nil {
			for tagName, fieldDescr := range fields {
				if tagName == self.GetIdentityFieldName() {
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

		if idI, err := self.formatAndValidateId(record.ID, op, record); err == nil {
			record.ID = idI

			// make sure the corresponding ID in the input struct matches
			if fieldDescr, ok := fields[idFieldName]; ok {
				if err := typeutil.SetValue(fieldDescr.ReflectField, idI); err != nil {
					return nil, fmt.Errorf("failed to writeback value to %q: %v", idFieldName, err)
				}
			}
		} else {
			return nil, err
		}

		// validate whole record (if specified)
		if err := self.ValidateRecord(record, op); err != nil {
			return nil, err
		}

		return record, nil
	} else {
		return nil, err
	}
}

// Convert the given record into a map.
func (self *Collection) MapFromRecord(record *Record, fields ...string) (map[string]interface{}, error) {
	rv := make(map[string]interface{})

	for _, field := range self.Fields {
		if len(fields) > 0 && !sliceutil.ContainsString(fields, field.Name) {
			continue
		}

		if dv := field.GetDefaultValue(); dv != nil {
			rv[field.Name] = dv
		}
	}

	if record != nil {
		if record.ID != nil {
			if id, err := self.formatAndValidateId(record.ID, RetrieveOperation, record); err == nil {
				rv[self.GetIdentityFieldName()] = id
			} else {
				return nil, err
			}
		}

		if len(self.Fields) > 0 {
			for _, field := range self.Fields {
				if v := record.Get(field.Name); v != nil {
					if len(fields) > 0 && !sliceutil.ContainsString(fields, field.Name) {
						continue
					}

					rv[field.Name] = v
				}
			}
		} else {
			for k, v := range record.Fields {
				rv[k] = v
			}
		}
	}

	return rv, nil
}

// Validate the given record against all Field and Collection validators.
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

// Verifies that the schema passes some basic sanity checks.
func (self *Collection) Check() error {
	var merr error

	for i, field := range self.Fields {
		if field.Name == `` {
			merr = log.AppendError(merr, fmt.Errorf("collection[%s] field #%d cannot have an empty name", self.Name, i))
		}

		if ParseFieldType(string(field.Type)) == `` {
			merr = log.AppendError(merr, fmt.Errorf("collection[%s] field[%s]: invalid type %q", self.Name, field.Name, field.Type))
		}
	}

	return merr
}

// Determine the differences (if any) between this Collection definition and another.
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

	if self.GetIdentityFieldName() != actual.GetIdentityFieldName() {
		differences = append(
			differences,
			SchemaDelta{
				Type:       CollectionDelta,
				Issue:      CollectionKeyNameIssue,
				Message:    `does not match`,
				Collection: self.Name,
				Parameter:  `IdentityField`,
				Desired:    self.GetIdentityFieldName(),
				Actual:     actual.GetIdentityFieldName(),
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
