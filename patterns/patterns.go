package patterns

import (
	"fmt"
	"github.com/ghetzel/pivot/util"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger(`pivot`)

type PatternType int

const (
	RecordPattern PatternType = iota
)

func RegisterHandlers(backendName string, pType PatternType, p interface{}) ([]util.Endpoint, error) {
	var err error

	handlers := make([]util.Endpoint, 0)

	switch pType {
	case RecordPattern:
		var pattern IRecordAccessPattern
		pattern = p.(IRecordAccessPattern)

		if pattern != nil {
			handlers, err = registerRecordAccessPatternHandlers(backendName, pattern, p)
		}

	default:
		err = fmt.Errorf("Cannot register routes for unknown access pattern %T", p)
	}

	if err != nil {
		return nil, err
	}

	return handlers, nil
}
