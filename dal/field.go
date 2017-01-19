package dal

import (
	"fmt"
	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/stringutil"
	"time"
)

type Field struct {
	Name         string             `json:"name"`
	Description  string             `json:"description,omitempty"`
	Type         Type               `json:"type"`
	Length       int                `json:"length,omitempty"`
	Precision    int                `json:"precision,omitempty"`
	Identity     bool               `json:"identity,omitempty"`
	Key          bool               `json:"key,omitempty"`
	Required     bool               `json:"required,omitempty"`
	Unique       bool               `json:"unique,omitempty"`
	DefaultValue interface{}        `json:"default,omitempty"`
	NativeType   string             `json:"native_type,omitempty"`
	Validator    FieldValidatorFunc `json:"-"`
	Formatter    FieldFormatterFunc `json:"-"`
}

func (self *Field) ConvertValue(in interface{}) (interface{}, error) {
	if in == nil {
		return nil, nil
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
		convertType = stringutil.Time
	default:
		return in, nil
	}

	return stringutil.ConvertTo(convertType, in)
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
	default:
		return make([]byte, 0)
	}
}

func (self *Field) Validate(value interface{}) error {
	if self.Validator == nil {
		return nil
	} else {
		return self.Validator(value)
	}
}

func (self *Field) Diff(other *Field) []SchemaDelta {
	diff := make([]SchemaDelta, 0)
	mine := structs.New(self)
	theirs := structs.New(other)

	for _, myField := range mine.Fields() {
		if myField.IsExported() {
			theirField, _ := theirs.FieldOk(myField.Name())

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

				fallthrough
			default:
				myV := myField.Value()
				theirV := theirField.Value()

				if myV != theirV {
					diff = append(diff, SchemaDelta{
						Type:      FieldDelta,
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
