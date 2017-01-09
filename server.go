package pivot

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/util"
	"github.com/husobee/vestigo"
	"github.com/urfave/negroni"
	"net/http"
	"strings"
	"time"
)

const DEFAULT_SERVER_ADDRESS = `127.0.0.1`
const DEFAULT_SERVER_PORT = 29029

var DefaultResultLimit = 25

type Server struct {
	Address          string
	Port             int
	ConnectionString string
	backend          backends.Backend
	router           *vestigo.Router
	server           *negroni.Negroni
	endpoints        []util.Endpoint
	routeMap         map[string]util.EndpointResponseFunc
}

func NewServer(connectionString ...string) *Server {
	return &Server{
		Address:          DEFAULT_SERVER_ADDRESS,
		Port:             DEFAULT_SERVER_PORT,
		ConnectionString: connectionString[0],
		endpoints:        make([]util.Endpoint, 0),
		routeMap:         make(map[string]util.EndpointResponseFunc),
	}
}

func (self *Server) ListenAndServe() error {
	if conn, err := dal.ParseConnectionString(self.ConnectionString); err == nil {
		if backend, err := backends.MakeBackend(conn); err == nil {
			self.backend = backend

			if err := self.backend.Initialize(); err == nil {
				log.Debugf("Initialized backend %T", self.backend)
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}

	self.server = negroni.New()
	self.router = vestigo.NewRouter()

	self.router.SetGlobalCors(&vestigo.CorsAccessControl{
		AllowOrigin:      []string{"*"},
		AllowCredentials: true,
		AllowMethods:     []string{`GET`, `POST`, `PUT`, `DELETE`},
		MaxAge:           3600 * time.Second,
		AllowHeaders:     []string{"*"},
	})

	self.server.Use(negroni.NewRecovery())
	self.server.Use(negroni.NewStatic(http.Dir(`public`)))
	self.server.UseHandler(self.router)

	if err := self.setupRoutes(); err != nil {
		return err
	}

	self.server.Run(fmt.Sprintf("%s:%d", self.Address, self.Port))
	return nil
}

func (self *Server) Respond(w http.ResponseWriter, code int, payload interface{}, err error) {
	response := make(map[string]interface{})
	response[`responded_at`] = time.Now().Format(time.RFC3339)
	response[`payload`] = payload

	if code >= http.StatusBadRequest {
		response[`success`] = false

		if err != nil {
			response[`error`] = err.Error()
		}
	} else {
		response[`success`] = true
	}

	if data, err := json.Marshal(response); err == nil {
		w.Header().Set(`Content-Type`, `application/json`)
		w.WriteHeader(code)
		w.Write(data)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (self *Server) setupRoutes() error {
	self.router.Get(`/collections/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)

			if collection, err := self.backend.GetCollection(name); err == nil {
				self.Respond(w, http.StatusOK, collection, nil)
			} else {
				self.Respond(w, http.StatusNotFound, nil, err)
			}
		})

	self.router.Get(`/collections/:collection/where/*urlquery`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			query := vestigo.Param(req, `_name`)

			limit := 0
			offset := 0

			if i, err := self.qsInt(req, `limit`); err == nil {
				if i > 0 {
					limit = int(i)
				} else {
					limit = DefaultResultLimit
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
				return
			}

			if i, err := self.qsInt(req, `offset`); err == nil {
				offset = int(i)
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
				return
			}

			if f, err := filter.Parse(query); err == nil {
				f.Limit = limit
				f.Offset = offset

				if search := self.backend.WithSearch(); search != nil {
					if recordset, err := search.Query(name, f); err == nil {
						self.Respond(w, http.StatusOK, recordset, nil)
					} else {
						self.Respond(w, http.StatusInternalServerError, nil, err)
					}
				} else {
					self.Respond(w, http.StatusBadRequest, nil, fmt.Errorf("Backend %T does not support complex queries."))
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	self.router.Get(`/collections/:collection/list/*fields`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			fieldNames := vestigo.Param(req, `_name`)

			f := filter.All

			if v := req.URL.Query().Get(`q`); v != `` {
				if fV, err := filter.Parse(v); err == nil {
					f = fV
				} else {
					self.Respond(w, http.StatusBadRequest, nil, err)
				}
			}

			if search := self.backend.WithSearch(); search != nil {
				fields := strings.TrimPrefix(fieldNames, `/`)

				if recordset, err := search.ListValues(name, strings.Split(fields, `/`), f); err == nil {
					self.Respond(w, http.StatusOK, recordset, nil)
				} else {
					self.Respond(w, http.StatusInternalServerError, nil, err)
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, fmt.Errorf("Backend %T does not support complex queries."))
			}
		})

	self.router.Delete(`/collections/:collection/where/*urlquery`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			query := vestigo.Param(req, `_name`)

			if f, err := filter.Parse(query); err == nil {
				if err := self.backend.Delete(name, f); err == nil {
					self.Respond(w, http.StatusNoContent, nil, nil)
				} else {
					self.Respond(w, http.StatusBadRequest, nil, err)
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	self.router.Get(`/collections/:collection/:id`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			id := vestigo.Param(req, `id`)

			if record, err := self.backend.Retrieve(name, id); err == nil {
				self.Respond(w, http.StatusOK, record, nil)
			} else {
				self.Respond(w, http.StatusInternalServerError, nil, err)
			}
		})

	self.router.Delete(`/collections/:collection/:id`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			id := vestigo.Param(req, `id`)

			if err := self.backend.Delete(name, id); err == nil {
				self.Respond(w, http.StatusNoContent, nil, nil)
			} else {
				self.Respond(w, http.StatusInternalServerError, nil, err)
			}
		})

	self.router.Post(`/collections/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			var recordset dal.RecordSet
			name := vestigo.Param(req, `collection`)

			if err := json.NewDecoder(req.Body).Decode(&recordset); err == nil {
				if err := self.backend.Insert(name, &recordset); err == nil {
					self.Respond(w, http.StatusNoContent, nil, nil)
				} else {
					self.Respond(w, http.StatusInternalServerError, nil, err)
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	self.router.Put(`/collections/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			var recordset dal.RecordSet
			name := vestigo.Param(req, `collection`)

			if err := json.NewDecoder(req.Body).Decode(&recordset); err == nil {
				if err := self.backend.Update(name, &recordset); err == nil {
					self.Respond(w, http.StatusNoContent, nil, nil)
				} else {
					self.Respond(w, http.StatusInternalServerError, nil, err)
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	self.router.Get(`/schema`,
		func(w http.ResponseWriter, req *http.Request) {
			if names, err := self.backend.ListCollections(); err == nil {
				self.Respond(w, http.StatusOK, names, nil)
			} else {
				self.Respond(w, http.StatusInternalServerError, nil, err)
			}
		})

	self.router.Post(`/schema`,
		func(w http.ResponseWriter, req *http.Request) {
			var collection dal.Collection

			if err := json.NewDecoder(req.Body).Decode(&collection); err == nil {
				if err := self.backend.CreateCollection(&collection); err == nil {
					self.Respond(w, http.StatusNoContent, nil, nil)
				} else {
					self.Respond(w, http.StatusInternalServerError, nil, err)
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	self.router.Get(`/schema/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)

			if collection, err := self.backend.GetCollection(name); err == nil {
				self.Respond(w, http.StatusOK, collection, nil)
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	return nil
}

func (self *Server) qsInt(req *http.Request, key string) (int64, error) {
	if v := req.URL.Query().Get(key); v != `` {
		if i, err := stringutil.ConvertToInteger(v); err == nil {
			return i, nil
		} else {
			return 0, fmt.Errorf("%s: %v", key, err)
		}
	}

	return 0, nil
}
