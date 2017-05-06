package dal

import (
	"fmt"
	"strings"
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
)

func (self Type) String() string {
	return string(self)
}

type FieldOperation int

const (
	PersistOperation FieldOperation = iota
	RetrieveOperation
)

type FieldValidatorFunc func(interface{}) error
type FieldFormatterFunc func(interface{}, FieldOperation) (interface{}, error)

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
	Type       DeltaType
	Issue      DeltaIssue
	Message    string
	Collection string
	Name       string
	Parameter  string
	Desired    interface{}
	Actual     interface{}
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
