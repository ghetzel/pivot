package pivot

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/util"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strings"
)

// Retrieves a set of endpoints for all backends and wraps them in a common handler
// format that makes the response format consistent across all endpoints.
//
func (self *Server) setupBackendRoutes() error {
	typesVisited := make([]string, 0)

	for name, backend := range Backends {
		backendType := backend.GetDataset().Type

		self.setupGlobalApiRoutes()
		self.setupAdminRoutesForBackend(backend)

		log.Debugf("Registering routes for backend: '%s'", name)

		if e, err := self.registerBackendRoutes(backend); err == nil {
			self.endpoints = append(self.endpoints, e...)
		} else {
			return err
		}

		if !sliceutil.ContainsString(typesVisited, backendType) {
			self.router.ServeFiles(fmt.Sprintf("/resources/%s/*filepath", backendType), http.Dir(fmt.Sprintf("public/res/%s", backendType)))
			typesVisited = append(typesVisited, backendType)
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
			routeKey := routeMapKey(request.Method, fullPath)

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

							// never ever allow a "200 OK" error to get by; if there was an error,
							// it should present as such in the response status
							if handlerErr != nil && status < http.StatusBadRequest {
								status = http.StatusInternalServerError
							}

							// perform post-processing of response body
							if processed, err := self.postProcessResponsePayload(payload, backend, request, paramsMap); err == nil {

								self.Respond(w, status, processed, handlerErr)
							} else {
								self.Respond(w, http.StatusBadRequest, map[string]interface{}{
									`route`: routeKey,
								}, fmt.Errorf("Post-processing failed: %s", err))
							}
						} else {
							// Not Implemented
							self.Respond(w, http.StatusNotImplemented, map[string]interface{}{
								`route`: routeKey,
							}, fmt.Errorf("Cannot find a handler for route %s", routeKey))
						}
					} else {
						// Unavailable
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

// Adds routes that define the top-level API for Pivot
//
func (self *Server) setupGlobalApiRoutes() {
	self.router.GET(`/api/status`, func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		self.Respond(w, http.StatusOK, util.Status{
			OK:          true,
			Application: util.ApplicationName,
			Version:     util.ApplicationVersion,
		}, nil)
	})

	self.router.GET(`/api/backends`, func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		self.Respond(w, http.StatusOK, Backends, nil)
	})
}

// Adds routes for adminstering and controlling the given backend.
//
func (self *Server) setupAdminRoutesForBackend(backend backends.IBackend) {
	self.router.PUT(urlForBackend(backend.GetName(), `/suspend`), func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		parts := strings.Split(req.URL.Path, `/`)

		if len(parts) >= 4 {
			backendName := parts[3]

			if backend, ok := Backends[backendName]; ok {
				backend.Suspend()
				self.Respond(w, http.StatusAccepted, nil, nil)
			} else {
				self.Respond(w, http.StatusNotFound, nil, fmt.Errorf("Backend '%s' does not exist", backendName))
			}
		}
	})

	self.router.PUT(urlForBackend(backend.GetName(), `/resume`), func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		parts := strings.Split(req.URL.Path, `/`)

		if len(parts) >= 4 {
			backendName := parts[3]

			if backend, ok := Backends[backendName]; ok {
				backend.Resume()
				self.Respond(w, http.StatusAccepted, nil, nil)
			} else {
				self.Respond(w, http.StatusNotFound, nil, fmt.Errorf("Backend '%s' does not exist", backendName))
			}
		}
	})

	self.router.PUT(urlForBackend(backend.GetName(), `/disconnect`), func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		parts := strings.Split(req.URL.Path, `/`)

		if len(parts) >= 4 {
			backendName := parts[3]

			if backend, ok := Backends[backendName]; ok {
				backend.Disconnect()
				self.Respond(w, http.StatusAccepted, nil, nil)
			} else {
				self.Respond(w, http.StatusNotFound, nil, fmt.Errorf("Backend '%s' does not exist", backendName))
			}
		}
	})

	self.router.PUT(urlForBackend(backend.GetName(), `/connect`), func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		parts := strings.Split(req.URL.Path, `/`)

		if len(parts) >= 4 {
			backendName := parts[3]

			if backend, ok := Backends[backendName]; ok {
				if err := backend.Connect(); err == nil {
					self.Respond(w, http.StatusAccepted, nil, nil)
				} else {
					self.Respond(w, http.StatusInternalServerError, nil, err)
				}
			} else {
				self.Respond(w, http.StatusNotFound, nil, fmt.Errorf("Backend '%s' does not exist", backendName))
			}
		}
	})
}

// Returns all endpoints that apply to a given backend
//
func (self *Server) registerBackendRoutes(b interface{}) ([]util.Endpoint, error) {
	switch b.(type) {
	case backends.IBackend:
		backend := b.(backends.IBackend)

		if endpoints, err := backends.RegisterHandlers(backend); err == nil {
			return endpoints, nil
		} else {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("Cannot register backend; %T does not implement backend.IBackend", b)
	}
}

// STUB: will provide the opportunity for post-processing of response before returning it
func (self *Server) postProcessResponsePayload(in interface{}, backend backends.IBackend, request *http.Request, params map[string]string) (interface{}, error) {
	switch in.(type) {
	case *dal.RecordSet:
		out := in.(*dal.RecordSet)

		if err := backend.ProcessPayload(backends.ResponsePayload, out, request); err == nil {
			return out, nil
		} else {
			return in, err
		}
	}

	// default action is to passthrough successfully
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