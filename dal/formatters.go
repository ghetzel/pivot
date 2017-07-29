package dal

import (
	"fmt"

	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/satori/go.uuid"
)

func GenerateUUID(value interface{}, _ FieldOperation) (interface{}, error) {
	if record, ok := value.(*Record); ok {
		value = record.ID
	}

	if typeutil.IsZero(value) {
		value = uuid.NewV4().String()
	}

	return value, nil
}

func DeriveFromFields(format string, fields ...string) FieldFormatterFunc {
	return func(input interface{}, _ FieldOperation) (interface{}, error) {
		if record, ok := input.(*Record); ok {
			values := make([]interface{}, len(fields))

			for i, field := range fields {
				values[i] = record.Get(field)
			}

			return fmt.Sprintf(format, values...), nil
		} else {
			return nil, fmt.Errorf("DeriveFromFields formatter requires a *dal.Record argument, got %T", input)
		}
	}
}
