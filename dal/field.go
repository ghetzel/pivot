package dal

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"time"
)

type Field struct {
	Name         string      `json:"name"`
	Type         Type        `json:"type"`
	Length       int         `json:"length,omitempty"`
	Precision    int         `json:"precision,omitempty"`
	Identity     bool        `json:"identity,omitempty"`
	Key          bool        `json:"key,omitempty"`
	Required     bool        `json:"required,omitempty"`
	Unique       bool        `json:"unique,omitempty"`
	DefaultValue interface{} `json:"default,omitempty"`
	NativeType   string      `json:"native_type,omitempty"`
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
