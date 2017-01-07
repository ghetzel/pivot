package dal

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
