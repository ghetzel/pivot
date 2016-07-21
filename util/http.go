package util

import (
	"net/http"
)

type EndpointResponseFunc func(*http.Request, map[string]string) (int, interface{}, error)

type Endpoint struct {
	BackendName string
	Method      string
	Path        string
	Handler     EndpointResponseFunc
}
