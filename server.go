package pivot

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/util"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
	"github.com/urfave/negroni"
	"net/http"
	"time"
)

const DEFAULT_SERVER_ADDRESS = `127.0.0.1`
const DEFAULT_SERVER_PORT = 29029

type Server struct {
	Address          string
	Port             int
	ConnectionString string
	backend          backends.Backend
	corsHandler      *cors.Cors
	router           *httprouter.Router
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
	self.router = httprouter.New()

	self.corsHandler = cors.New(cors.Options{
		AllowedOrigins:   []string{`*`},
		AllowedMethods:   []string{`GET`, `POST`},
		AllowedHeaders:   []string{`*`},
		AllowCredentials: true,
	})

	self.server.Use(negroni.NewRecovery())
	self.server.Use(negroni.NewStatic(http.Dir(`public`)))
	self.server.Use(self.corsHandler)
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
	self.router.GET(`/query/:collection/where/*urlquery`,
		func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
			if f, err := filter.Parse(params.ByName(`urlquery`)); err == nil {
				if search := self.backend.WithSearch(); search != nil {
					if recordset, err := search.Query(params.ByName(`collection`), f); err == nil {
						self.Respond(w, http.StatusOK, recordset, nil)
					} else {
						self.Respond(w, http.StatusInternalServerError, nil, err)
					}
				}else{
					self.Respond(w, http.StatusBadRequest, nil, fmt.Errorf("Backend %T does not support complex queries."))
				}
			} else {
				self.Respond(w, http.StatusInternalServerError, nil, err)
			}
		})

	self.router.POST(`/query/:collection`,
		func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
			var recordset dal.RecordSet

			if err := json.NewDecoder(req.Body).Decode(&recordset); err == nil {
				if err := self.backend.InsertRecords(params.ByName(`collection`), &recordset); err == nil {
					self.Respond(w, http.StatusNoContent, nil, nil)
				} else {
					self.Respond(w, http.StatusInternalServerError, nil, err)
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	self.router.PUT(`/query/:collection`,
		func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
			var recordset dal.RecordSet

			if err := json.NewDecoder(req.Body).Decode(&recordset); err == nil {
				if err := self.backend.UpdateRecords(params.ByName(`collection`), &recordset); err == nil {
					self.Respond(w, http.StatusNoContent, nil, nil)
				} else {
					self.Respond(w, http.StatusInternalServerError, nil, err)
				}
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	self.router.DELETE(`/query/:collection/where/*urlquery`,
		func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
			var identities []dal.Identity

			if err := json.NewDecoder(req.Body).Decode(&identities); err == nil {
				if err := self.backend.DeleteRecords(params.ByName(`collection`), identities); err == nil {
					self.Respond(w, http.StatusNoContent, nil, nil)
				} else {
					self.Respond(w, http.StatusBadRequest, nil, err)
				}
			} else {
				self.Respond(w, http.StatusInternalServerError, nil, err)
			}
		})

	self.router.GET(`/schema/:collection`,
		func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
			if collection, err := self.backend.GetCollection(params.ByName(`collection`)); err == nil {
				self.Respond(w, http.StatusOK, collection, nil)
			} else {
				self.Respond(w, http.StatusBadRequest, nil, err)
			}
		})

	return nil
}
