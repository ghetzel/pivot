package dal

import (
	"fmt"
	"strings"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
)

type Type string

const (
	StringType  Type = `str`
	AutoType         = `auto`
	BooleanType      = `bool`
	IntType          = `int`
	FloatType        = `float`
	TimeType         = `time`
	ObjectType       = `object`
	RawType          = `raw`
	ArrayType        = `array`
)

func (self Type) String() string {
	return string(self)
}

func ParseFieldType(in string) Type {
	switch in {
	case `str`:
		return StringType
	case `bool`:
		return BooleanType
	case `int`:
		return IntType
	case `float`:
		return FloatType
	case `time`:
		return TimeType
	case `object`:
		return ObjectType
	case `raw`:
		return RawType
	case `array`:
		return ArrayType
	default:
		return ``
	}
}

type FieldOperation int

const (
	PersistOperation FieldOperation = iota
	RetrieveOperation
)

type FieldValidatorFunc func(interface{}) error
type FieldFormatterFunc func(interface{}, FieldOperation) (interface{}, error)
type CollectionValidatorFunc func(*Record) error

type DeltaType string

const (
	CollectionDelta DeltaType = `collection`
	FieldDelta                = `field`
)

type DeltaIssue int

const (
	UnknownIssue DeltaIssue = iota
	CollectionNameIssue
	CollectionKeyNameIssue
	CollectionKeyTypeIssue
	FieldMissingIssue
	FieldNameIssue
	FieldLengthIssue
	FieldTypeIssue
	FieldPropertyIssue
)

type SchemaDelta struct {
	Type           DeltaType
	Issue          DeltaIssue
	Message        string
	Collection     string
	Name           string
	Parameter      string
	Desired        interface{}
	Actual         interface{}
	ReferenceField *Field
}

func (self SchemaDelta) DesiredField(from Field) *Field {
	field := &from
	maputil.M(field).Set(self.Parameter, self.Desired)

	log.Noticef("DESIRED: %+v", self.Actual)

	return field
}

func (self SchemaDelta) String() string {
	msg := fmt.Sprintf("%s '%s'", strings.Title(string(self.Type)), self.Name)

	if self.Parameter != `` {
		msg += fmt.Sprintf(", parameter '%s'", self.Parameter)
	}

	msg += fmt.Sprintf(": %s", self.Message)

	dV := fmt.Sprintf("%v", self.Desired)
	aV := fmt.Sprintf("%v", self.Actual)

	if len(dV) <= 12 && len(aV) <= 12 {
		msg += fmt.Sprintf(" (desired: %v, actual: %v)", dV, aV)
	}

	return msg
}

type Migratable interface {
	Migrate(diff []*SchemaDelta) error
}
