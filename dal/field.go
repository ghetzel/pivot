package dal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/typeutil"
)

var DefaultFieldCodec = `json`
var IntIsProbablyUnixEpochSeconds int64 = 4294967296

type Field struct {
	// The name of the field
	Name string `json:"name"`

	// A description of the field used in help text
	Description string `json:"description,omitempty"`

	// The data type of the field
	Type Type `json:"type"`

	// For complex field types (tuples, objects); the data type of the key portion
	KeyType Type `json:"keytype,omitempty"`

	// For complex field types (arrays, sets, lists); the data type of the contained values
	Subtype Type `json:"subtype,omitempty"`

	// The length constraint for values in the field (where supported)
	Length int `json:"length,omitempty"`

	// The precision of stored values in the field (where supported)
	Precision int `json:"precision,omitempty"`

	// Whether the field is an identity field (don't use this, configure the identity on the
	// Collection instead)
	Identity bool `json:"identity,omitempty"`

	// Whether the field is a key field in a composite key Collection
	Key bool `json:"key,omitempty"`

	// Whether the field can store a null/empty value
	Required bool `json:"required,omitempty"`

	// Enforces that the field value must be unique across the entire Collection (where supported)
	Unique bool `json:"unique,omitempty"`

	// The name of a group of unique fields that, taken together, must be unique across the entire
	// Collection (where supported)
	UniqueGroup string `json:"unique_group,omitempty"`

	// The default value of the field is one is not explicitly specified.  Can be any type or a
	// function that takes zero arguments and returns a single value.
	DefaultValue interface{} `json:"default,omitempty"`

	// Represents the native datatype of the underlying Backend object (read only)
	NativeType string `json:"native_type,omitempty"`

	// Specify that the field should not be modified.  This is not enforced in Pivot, but rather
	// serves as a note to applications implementing interactions with the Pivot API.
	NotUserEditable bool `json:"not_user_editable"`

	// Whether this field's validator(s) should be used to validate data being retrieved from the
	// backend.  Invalid data (possibly created outside of Pivot) will cause Retrieve() calls to
	// return a validation error.
	ValidateOnPopulate bool `json:"validate_on_populate,omitempty"`

	// A function that is used to validate the field's value before performing any create, update,
	// and (optionally) retrieval operations.
	Validator FieldValidatorFunc `json:"-"`

	// A function that can modify values before any create or update operations.  Formatters run
	// before Validators, giving users the opportunity to ensure a valid value is in the data
	// structure before validation runs.
	Formatter FieldFormatterFunc `json:"-"`

	// A declarative form of the Validator configuration that uses pre-defined validators. Primarily
	// used when storing schema declarations in external JSON files.
	ValidatorConfig map[string]interface{} `json:"validators,omitempty"`

	// A declarative form of the Formatter configuration that uses pre-defined validators. Primarily
	// used when storing schema declarations in external JSON files.
	FormatterConfig map[string]interface{} `json:"formatters,omitempty"`

	// Used to store the order this field appears in the source database.
	Index int `json:"index,omitempty"`
}

func (self *Field) normalizeType(in interface{}) (interface{}, error) {
	variant := typeutil.V(in)

	switch self.Type {
	case StringType:
		in = variant.String()
	case BooleanType:
		in = variant.Bool()
	case IntType:
		in = variant.Int()
	case FloatType:
		in = variant.Float()
	case ArrayType:
		if in == nil {
			return nil, nil
		} else if typeutil.IsArray(in) {
			if arr := sliceutil.Sliceify(in); len(arr) > 0 {
				return arr, nil
			} else {
				return make([]interface{}, 0), nil
			}
		} else {
			var raw []byte
			var arr []interface{}

			if typeutil.IsKindOfString(in) {
				raw = []byte(typeutil.String(in))
			} else if r, ok := in.([]byte); ok {
				raw = r
			} else if r, ok := in.([]uint8); ok {
				raw = []byte(r)
			} else {
				return nil, fmt.Errorf("Cannot use %T as an ArrayType input", in)
			}

			if err := json.Unmarshal(raw, &arr); err == nil {
				return arr, nil
			} else {
				return nil, err
			}
		}
	case ObjectType:
		if in != nil {
			if native, ok := in.(map[string]interface{}); ok {
				return native, nil
			} else if typeutil.IsMap(in) {
				return variant.MapNative(), nil
			} else {
				var raw []byte
				var obj map[string]interface{}

				if typeutil.IsStruct(in) {
					if r, err := json.Marshal(in); err == nil {
						raw = r
					} else {
						return nil, fmt.Errorf("Cannot convert %T to map: %v", in, err)
					}
				} else if typeutil.IsKindOfString(in) {
					raw = []byte(typeutil.String(in))
				} else if r, ok := in.([]byte); ok {
					raw = r
				} else if r, ok := in.([]uint8); ok {
					raw = []byte(r)
				} else {
					return nil, fmt.Errorf("Cannot use %T as an ObjectType input", in)
				}

				if err := json.Unmarshal(raw, &obj); err == nil {
					return obj, nil
				} else {
					return nil, err
				}
			}
		}
	case TimeType:
		if in == nil {
			in = time.Time{}
		} else if inInt64, ok := in.(int64); ok {
			// parse incoming int64s as epoch or epoch milliseconds
			if inInt64 < IntIsProbablyUnixEpochSeconds {
				in = time.Unix(inInt64, 0)
			} else {
				in = time.Unix(0, inInt64)
			}
		} else {
			in = variant.Time()
		}
	default:
		switch strings.ToLower(fmt.Sprintf("%v", in)) {
		case `null`, `nil`:
			in = nil
		}
	}

	return in, nil
}

func (self *Field) ConvertValue(in interface{}) (interface{}, error) {
	if norm, err := self.normalizeType(in); err == nil {
		in = norm
	} else {
		return nil, err
	}

	// decide what to do with the now-normalized type
	if typeutil.IsZero(in) {
		if self.DefaultValue != nil {
			return self.GetDefaultValue(), nil

		} else if self.Type == BooleanType && in != nil {
			return false, nil

		} else if self.Required {
			return self.GetTypeInstance(), nil
		}
	}

	return in, nil
}

func (self *Field) GetDefaultValue() interface{} {
	if self.DefaultValue == nil {
		return nil
	} else if typeutil.IsFunctionArity(self.DefaultValue, 0, 1) {
		if values := reflect.ValueOf(self.DefaultValue).Call(make([]reflect.Value, 0)); len(values) == 1 {
			if norm, err := self.normalizeType(values[0].Interface()); err == nil {
				return norm
			}
		}
	}

	if norm, err := self.normalizeType(self.DefaultValue); err == nil {
		return norm
	} else {
		return nil
	}
}

func (self *Field) GetTypeInstance() interface{} {
	switch self.Type {
	case StringType:
		return ``
	case BooleanType:
		return false
	case IntType:
		return int64(0)
	case FloatType:
		return float64(0.0)
	case TimeType:
		return time.Time{}
	case ObjectType:
		return make(map[string]interface{})
	case ArrayType:
		return make([]interface{}, 0)
	default:
		return make([]byte, 0)
	}
}

func (self *Field) Validate(value interface{}) error {
	// automatically validate that required fields aren't being given a nil value
	if self.Required && value == nil {
		return fmt.Errorf("field %q is required", self.Name)
	}

	if self.Validator == nil {
		return nil
	} else if err := self.Validator(value); err != nil {
		return fmt.Errorf("validation error: %v", err)
	} else {
		return nil
	}
}

func (self *Field) Format(value interface{}, op FieldOperation) (interface{}, error) {
	if self.Formatter == nil {
		return value, nil
	} else {
		if v, err := self.Formatter(value, op); err == nil {
			return v, nil
		} else {
			return v, fmt.Errorf("formatter error: %v", err)
		}
	}
}

func (self *Field) Diff(other *Field) []SchemaDelta {
	diff := make([]SchemaDelta, 0)
	mine := structs.New(self)
	theirs := structs.New(other)

	for _, myField := range mine.Fields() {
		if myField.IsExported() {
			theirField, _ := theirs.FieldOk(myField.Name())
			deltaIssue := UnknownIssue

			switch myField.Name() {
			// skip parameters:
			//
			// 	NativeType:
			//		this is generally expected to be an output value from the database and not specified in schema definitions
			//  Description:
			//		this is largely for the use of the client application and won't always have a backend-persistent counterpart
			//  DefaultValue:
			//		this is a value that is interpreted by the backend and may not be retrievable after definition
			//
			case `NativeType`, `Description`, `DefaultValue`, `Validator`, `Formatter`, `FormatterConfig`, `ValidatorConfig`:
				continue
			case `Length`:
				if myV, ok := myField.Value().(int); ok {
					if theirV, ok := theirField.Value().(int); ok {
						// It is okay for lengths to exceed, but not be less than, our desired length
						if theirV < myV {
							diff = append(diff, SchemaDelta{
								Type:      FieldDelta,
								Issue:     FieldLengthIssue,
								Message:   `length is shorter than desired`,
								Name:      self.Name,
								Parameter: `Length`,
								Desired:   myV,
								Actual:    theirV,
							})
						}
					}
				}

				continue

			case `Type`:
				if myT, ok := myField.Value().(Type); ok {
					if theirT, ok := theirField.Value().(Type); ok {
						if myT != theirT {
							// ObjectType fields can be stored as a RawType on backends without
							// a native object type, so we treat raw fields as object fields
							if myT == ObjectType && theirT == RawType {
								continue
							}

							// some backends store times as integers, so allow that too
							if myT == TimeType && theirT == IntType {
								continue
							}
						}
					}
				}

				deltaIssue = FieldTypeIssue

				fallthrough
			default:
				myV := myField.Value()
				theirV := theirField.Value()

				if deltaIssue == UnknownIssue {
					deltaIssue = FieldPropertyIssue
				}

				if myV != theirV {
					diff = append(diff, SchemaDelta{
						Type:      FieldDelta,
						Issue:     deltaIssue,
						Message:   `values do not match`,
						Name:      self.Name,
						Parameter: theirField.Name(),
						Desired:   myV,
						Actual:    theirV,
					})
				}
			}
		}
	}

	if len(diff) == 0 {
		return nil
	}

	return diff
}

func (self *Field) MarshalJSON() ([]byte, error) {
	type Alias Field

	// this is a small pile or horrors that prevents infinite MarshalJSON stack
	// overflow recursion sadness
	if data, err := json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(self),
	}); err == nil {
		return data, nil
	} else {
		return json.Marshal(&struct {
			DefaultValue interface{} `json:"default,omitempty"`
			*Alias
		}{
			DefaultValue: nil,
			Alias:        (*Alias)(self),
		})
	}
}

func (self *Field) UnmarshalJSON(b []byte) error {
	type Alias Field

	// this is a small pile or horrors that prevents infinite UnmarshalJSON stack
	// overflow recursion sadness
	if err := json.Unmarshal(b, &struct {
		*Alias
	}{
		Alias: (*Alias)(self),
	}); err == nil {
		if len(self.FormatterConfig) > 0 {
			if formatter, err := FormatterFromMap(self.FormatterConfig); err == nil {
				self.Formatter = formatter
			} else {
				return fmt.Errorf("formatter error: %v", err)
			}
		}

		if len(self.ValidatorConfig) > 0 {
			if validator, err := ValidatorFromMap(self.ValidatorConfig); err == nil {
				self.Validator = validator
			} else {
				return fmt.Errorf("validator error: %v", err)
			}
		}

		return nil
	} else {
		return err
	}
}
