package pivot

import (
	"fmt"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/patterns"
	"github.com/ghetzel/pivot/util"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strings"
)

func (self *Server) setupBackendRoutes() error {
	for name, backend := range Backends {
		log.Debugf("Registering routes for backend: %q", name)

		if e, err := self.registerBackendRoutes(backend); err == nil {
			self.endpoints = append(self.endpoints, e...)
		} else {
			return err
		}
	}

	// for each endpoint...
	//
	for _, endpoint := range self.endpoints {
		fullPath := urlForBackend(endpoint.BackendName, endpoint.Path)
		self.routeMap[routeMapKey(endpoint.Method, fullPath)] = endpoint.Handler

		log.Debugf("Setting up handler for route: %-6s %s", endpoint.Method, fullPath)

		// wrap the endpoint in a handler that will automatically check the backend
		// for availability/enablement before processing the request
		//
		self.router.Handle(endpoint.Method, fullPath, func(w http.ResponseWriter, request *http.Request, params httprouter.Params) {
			parts := strings.Split(request.URL.Path, `/`)
			routeKey := routeMapKey(request.Method, request.URL.Path)

			if len(parts) >= 4 {
				backendName := parts[3]

				// retrieve the backend by name from the package global map
				if backend, ok := Backends[backendName]; ok {
					//  this condition will prevent any routes (except the root status route) from returning if the backend is unavailable
					if backend.IsAvailable() || len(parts) == 4 {
						if handler, ok := self.routeMap[routeKey]; ok && handler != nil {
							paramsMap := make(map[string]string)

							// convert the params to a map
							for _, param := range params {
								paramsMap[param.Key] = param.Value
							}

							// actually call the handler for this route
							status, payload, handlerErr := handler(request, paramsMap)

							// perform post-processing of response body
							if processed, err := self.postProcessResponsePayload(payload, request, paramsMap); err == nil {
								self.Respond(w, status, processed, handlerErr)
							} else {
								self.Respond(w, http.StatusBadRequest, map[string]interface{}{
									`route`: routeKey,
								}, fmt.Errorf("Post-processing failed: %s", err))
							}
						} else {
							//  respond Not Implemented
							self.Respond(w, http.StatusNotImplemented, map[string]interface{}{
								`route`: routeKey,
							}, fmt.Errorf("Cannot find a handler for route %s", routeKey))
						}
					} else {
						//  response Unavailable
						self.Respond(w, http.StatusServiceUnavailable, map[string]interface{}{
							`route`: routeKey,
						}, fmt.Errorf("The '%s' backend is unavailable at this time", backend.GetName()))
					}
				}
			}
		})
	}

	return nil
}

func (self *Server) registerBackendRoutes(b interface{}) ([]util.Endpoint, error) {
	switch b.(type) {
	case backends.IBackend:
		backend := b.(backends.IBackend)

		if endpoints, err := patterns.RegisterHandlers(backend.GetName(), b); err == nil {
			return endpoints, nil
		} else {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Cannot register backend; %T does not implement backend.IBackend", b)
	}
}

// STUB: will provide the opportunity for post-processing of response before returning it
func (self *Server) postProcessResponsePayload(in interface{}, request *http.Request, params map[string]string) (interface{}, error) {
	return in, nil
}

func routeMapKey(method string, path string) string {
	return fmt.Sprintf("%s:%s", strings.ToUpper(method), path)
}

func urlForBackend(name string, path string) string {
	var suffix string

	if len(path) > 0 && path != `/` {
		suffix = `/` + strings.TrimPrefix(path, `/`)
	}

	url := fmt.Sprintf("/api/backends/%s%s", name, suffix)
	return url
}
