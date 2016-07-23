package util

import (
	"github.com/op/go-logging"
	"net/http"
)

var log = logging.MustGetLogger(`pivot`)

type EndpointResponseFunc func(*http.Request, map[string]string) (int, interface{}, error)

type Endpoint struct {
	BackendName string
	Method      string
	Path        string
	Handler     EndpointResponseFunc
}
