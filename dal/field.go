package dal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
)

type Field struct {
	Name               string                 `json:"name"`
	Description        string                 `json:"description,omitempty"`
	Type               Type                   `json:"type"`
	KeyType            Type                   `json:"keytype,omitempty"`
	Subtype            Type                   `json:"subtype,omitempty"`
	Length             int                    `json:"length,omitempty"`
	Precision          int                    `json:"precision,omitempty"`
	Identity           bool                   `json:"identity,omitempty"`
	Key                bool                   `json:"key,omitempty"`
	Required           bool                   `json:"required,omitempty"`
	Unique             bool                   `json:"unique,omitempty"`
	DefaultValue       interface{}            `json:"default,omitempty"`
	NativeType         string                 `json:"native_type,omitempty"`
	ValidateOnPopulate bool                   `json:"validate_on_populate,omitempty"`
	Validator          FieldValidatorFunc     `json:"-"`
	Formatter          FieldFormatterFunc     `json:"-"`
	FormatterConfig    map[string]interface{} `json:"formatters,omitempty"`
	ValidatorConfig    map[string]interface{} `json:"validators,omitempty"`
}

func (self *Field) ConvertValue(in interface{}) (interface{}, error) {
	if typeutil.IsZero(in) {
		// non-required zero valued inputs are nilable, so return nil
		if self.Required {
			return self.GetTypeInstance(), nil
		} else {
			return nil, nil
		}
	}

	var convertType stringutil.ConvertType

	switch self.Type {
	case StringType:
		convertType = stringutil.String
	case BooleanType:
		if fmt.Sprintf("%v", in) == `1` {
			return true, nil
		} else if fmt.Sprintf("%v", in) == `0` {
			return false, nil
		}

		convertType = stringutil.Boolean
	case IntType:
		convertType = stringutil.Integer
	case FloatType:
		convertType = stringutil.Float
	case TimeType:
		// parse incoming int64s as epoch or epoch milliseconds
		if inInt64, ok := in.(int64); ok {
			if inInt64 < 4294967296 {
				return time.Unix(inInt64, 0), nil
			} else {
				return time.Unix(0, inInt64), nil
			}
		}

		convertType = stringutil.Time
	default:
		return in, nil
	}

	return stringutil.ConvertTo(convertType, in)
}

func (self *Field) GetDefaultValue() interface{} {
	if self.DefaultValue == nil {
		return nil
	} else if typeutil.IsFunctionArity(self.DefaultValue, 0, 1) {
		if values := reflect.ValueOf(self.DefaultValue).Call(make([]reflect.Value, 0)); len(values) == 1 {
			return values[0].Interface()
		}
	}

	return self.DefaultValue
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
		return &time.Time{}
	case ObjectType:
		return make(map[string]interface{})
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
			case `NativeType`, `Description`, `DefaultValue`, `Validator`, `Formatter`:
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
