package patterns

import (
	"fmt"
	"github.com/op/go-logging"
	"net/http"
	"strings"
)

var log = logging.MustGetLogger(`pivot`)

type PatternType int

const (
	RecordPattern PatternType = iota
)

func RegisterHandlers(mux *http.ServeMux, backendName string, p interface{}) error {
	var err error

	switch p.(type) {
	case IRecordAccessPattern:
		pattern := p.(IRecordAccessPattern)
		err = registerRecordAccessPatternHandlers(mux, backendName, pattern)

	default:
		err = fmt.Errorf("Cannot register routes for unknown access pattern %T", p)
	}

	if err != nil {
		return err
	}

	return nil
}

func urlForBackend(name string, path ...string) string {
	var suffix string

	if len(path) > 0 {
		suffix = `/` + strings.Join(path, `/`)
	}

	url := fmt.Sprintf("/api/backends/%s%s", name, suffix)
	log.Debugf("  %s", url)
	return url
}
