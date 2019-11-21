package dal

import (
	"fmt"
	"reflect"
	"time"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/structutil"
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

type Backend interface {
	GetCollection(collection string) (*Collection, error)
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
	// one-to-many relationships.  The definitions here will retrieve the associated records from
	// another, and those values will replace the value that is actually in this Collection's field.
	EmbeddedCollections []Relationship `json:"embed,omitempty"`

	// Allows for constraints to be applied to a collection.  In addition to informing Pivot about the
	// relationships between collections, this data is also used to enforce referential integrity for
	// backends that support such guarantees (e.g.: ACID-compliant RDBMS').
	Constraints []Constraint `json:"constraints,omitempty"`

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

	// Specifies that IDs should be automatically generated using a formatter function
	AutoIdentity string `json:"autoidentity"`

	// A function that validates the value of an identity key before create and update operations.
	// Operates the same as a Field Validator function.
	IdentityFieldValidator FieldValidatorFunc `json:"-"`

	// Allow backends to store internal information about the backing datasource for this collection.
	SourceURI string `json:"-"`

	// If specified, this function receives a copy of the populated record before create and update
	// operations, allowing for a last-chance validation of the record as a whole.  Use a pre-save
	// validator when validation requires checking multiple fields at once.
	PreSaveValidator CollectionValidatorFunc `json:"-"`

	// Specifies that this collection is a read-only view on data that is queried by the underlying database engine.
	View bool `json:"view,omitempty"`

	// Specify additional keywords in the view creation to modify how it is created.
	ViewKeywords string `json:"view_keywords,omitempty"`

	// A query object that is passed to the underlying database engine.
	ViewQuery interface{} `json:"view_query,omitempty"`

	recordType reflect.Type
	backend    Backend
}

// Create a new colllection definition with no fields.
func NewCollection(name string, fields ...Field) *Collection {
	if len(fields) == 0 {
		fields = make([]Field, 0)
	}

	return &Collection{
		Name:                   name,
		Fields:                 fields,
		IdentityField:          DefaultIdentityField,
		IdentityFieldType:      DefaultIdentityFieldType,
		IdentityFieldValidator: ValidateNotEmpty,
	}
}

// Set the backend for this collection.  The Backend interface in this package is a limited subset
// of the backends.Backend interface that avoids a circular dependency between the two packages.
// The intent is to allow Collections to retrieve details about other collections registered on the
// same backend.
func (self *Collection) SetBackend(backend Backend) {
	self.backend = backend
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

// Deprecated: this functionality has been removed.
func (self *Collection) SetRecordType(in interface{}) *Collection {
	return self
}

func (self *Collection) HasRecordType() bool {
	if self.recordType != nil {
		return true
	}

	return false
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

// Same as KeyFields, but returns only the field names
func (self *Collection) KeyFieldNames() (names []string) {
	for _, kf := range self.KeyFields() {
		names = append(names, kf.Name)
	}

	return
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
		if v, err := self.extractValueFromRelationship(&field, value, op); err == nil {
			value = v
		} else {
			return nil, err
		}

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

// Takes a value provided for either persistence to the backend, or as retrieved from the backend, and hammers
// it into the right shape based on this collection's Constraints.
func (self *Collection) extractValueFromRelationship(field *Field, input interface{}, op FieldOperation) (interface{}, error) {
	// if:
	//	- persisting to backend
	//	- AND this field has a relationship
	//	- AND we have a struct
	//	THEN we need to extract key(s) from that struct
	if constraint := field.BelongsToConstraint(); constraint != nil {
		if resolved := typeutil.ResolveValue(input); typeutil.IsStruct(resolved) {
			if relatedTo, err := self.GetRelatedCollection(constraint.Collection); err == nil {
				if relatedRecord, err := relatedTo.StructToRecord(input); err == nil {
					keys := relatedRecord.Keys(relatedTo)

					if len(keys) == 1 {
						return keys[0], nil
					} else {
						return keys, nil
					}
				} else {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("related collection %q: %v", constraint.Collection, err)
			}
		}
	}

	return input, nil
}

func (self *Collection) formatAndValidateId(id interface{}, op FieldOperation, record *Record) (interface{}, error) {
	switch self.AutoIdentity {
	case ``:
		break
	default:
		if fn, err := GetFormatter(self.AutoIdentity, nil); err == nil {
			if i, err := fn(id, PersistOperation); err == nil {
				id = i
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

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

func (self *Collection) EmptyRecord() *Record {
	record := NewRecord(nil)
	self.FillDefaults(record)

	return record
}

// Generates a Record suitable for persistence in a backend from the given struct.
func (self *Collection) StructToRecord(in interface{}) (*Record, error) {
	var idDesc *fieldDescription

	if err := validatePtrToStructType(in); err != nil {
		return nil, err
	}

	output := self.EmptyRecord()

	// if the argument is already a record, return it as-is
	if record, ok := in.(*Record); ok {
		output.ID = record.ID

		// this is a roundabout way of ensuring that generated IDs are written back to the record
		// we were given as input
		idDesc = record.identityFieldDescription()

		// we're returning the record we were given, but first we need to validate and format it
		for key, value := range record.Fields {
			if v, err := self.ValueForField(key, value, PersistOperation); err == nil {
				output.Set(key, v)
			} else if IsFieldNotFoundErr(err) {
				continue
			} else {
				return nil, err
			}
		}
	} else {
		idFieldName := getIdentityFieldName(in, self)

		// iterate through all struct fields and fill the output Record with values
		if err := structutil.FieldsFunc(in, func(field *reflect.StructField, value reflect.Value) error {
			desc := structFieldToDesc(field)
			desc.FieldValue = value
			desc.FieldType = value.Type()

			// skip omitted fields
			if desc.RecordKey == `-` {
				return nil
			}

			// don't clobber existing fields with empty data, except for bools, whose
			// zero value is meaningful
			if typeutil.IsZero(value) && value.Kind() != reflect.Bool && desc.OmitEmpty {
				return nil
			}

			// extract value from struct and put it in the appropriate place in the output Record
			if value.CanInterface() {
				fieldValue := value.Interface()

				if idFieldName != `` && field.Name == idFieldName {
					output.ID = fieldValue

					// set this so that generated IDs are written back to the struct we were given as input
					idDesc = desc
				} else if v, err := self.ValueForField(desc.RecordKey, fieldValue, PersistOperation); err == nil {
					output.Set(desc.RecordKey, v)
				} else if !IsFieldNotFoundErr(err) {
					return err
				}
			}

			return nil
		}); err != nil {
			return nil, err
		}
	}

	// validate the ID is cool and good
	if idI, err := self.formatAndValidateId(output.ID, PersistOperation, output); err == nil {
		output.ID = idI

		if idDesc != nil {
			if err := idDesc.Set(output.ID); err != nil {
				return nil, fmt.Errorf("failed to writeback ID to input object: %v", err)
			}
		}
	} else {
		return nil, err
	}

	// validate whole record
	if err := self.ValidateRecord(output, PersistOperation); err != nil {
		return nil, err
	}

	return output, nil
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
func (self *Collection) Diff(actual *Collection) []*SchemaDelta {
	differences := make([]*SchemaDelta, 0)

	if self.Name != actual.Name {
		differences = append(differences, &SchemaDelta{
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
			&SchemaDelta{
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
		differences = append(differences, &SchemaDelta{
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
			differences = append(differences, &SchemaDelta{
				Type:           FieldDelta,
				Issue:          FieldMissingIssue,
				Message:        `is missing`,
				Collection:     self.Name,
				Name:           myField.Name,
				ReferenceField: &myField,
			})
		}
	}

	if len(differences) == 0 {
		return nil
	}

	return differences
}

// Retrieve the set of all Constraints on this collection, both explicitly provided
// via the Constraints field, as well as constraints specified using the "BelongsTo"
// shorthand on Fields.
func (self *Collection) GetAllConstraints() (constraints []Constraint) {
	constraints = self.Constraints

	for _, field := range self.Fields {
		if proposed := field.BelongsToConstraint(); proposed != nil {
			var exists bool

			// run through existing constraints. if an equivalent constraint
			// already exists, replace it
			for i, c := range constraints {
				if c.Equal(proposed) {
					constraints[i] = *proposed
					exists = true
					break
				}
			}

			if !exists {
				constraints = append(constraints, *proposed)
			}
		}
	}

	return
}

// Retrieves a Collection by name from the backend this Collection is registered to.
func (self *Collection) GetRelatedCollection(name string) (*Collection, error) {
	if self.backend == nil {
		return nil, fmt.Errorf("Cannot retrieve related collection details without a registered backend.")
	} else {
		return self.backend.GetCollection(name)
	}
}

// Deprecated: use StructToRecord instead
func (self *Collection) MakeRecord(in interface{}, ops ...FieldOperation) (*Record, error) {
	return self.StructToRecord(in)
}
