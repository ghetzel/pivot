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

func RegisterHandlers(backendName string, p interface{}) ([]util.Endpoint, error) {
	var err error

	handlers := make([]util.Endpoint, 0)

	switch p.(type) {
	case IRecordAccessPattern:
		pattern := p.(IRecordAccessPattern)
		handlers, err = registerRecordAccessPatternHandlers(backendName, pattern, p)

	default:
		err = fmt.Errorf("Cannot register routes for unknown access pattern %T", p)
	}

	if err != nil {
		return nil, err
	}

	return handlers, nil
}
