package pivot

//go:generate esc -o static.go -pkg pivot -modtime 1500000000 -prefix ui ui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ghetzel/diecast"
	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/util"
	"github.com/husobee/vestigo"
	"github.com/urfave/negroni"
)

var DefaultAddress = `127.0.0.1`
var DefaultPort = 29029
var DefaultResultLimit = 25
var DefaultUiDirectory = `embedded`

type Server struct {
	Address          string
	ConnectionString string
	ConnectOptions   backends.ConnectOptions
	UiDirectory      string
	backend          backends.Backend
	endpoints        []util.Endpoint
	routeMap         map[string]util.EndpointResponseFunc
	schemaDefs       []string
}

func NewServer(connectionString ...string) *Server {
	return &Server{
		Address:          fmt.Sprintf("%s:%d", DefaultAddress, DefaultPort),
		ConnectionString: connectionString[0],
		UiDirectory:      DefaultUiDirectory,
		endpoints:        make([]util.Endpoint, 0),
		routeMap:         make(map[string]util.EndpointResponseFunc),
	}
}

func (self *Server) AddSchemaDefinition(filename string) {
	self.schemaDefs = append(self.schemaDefs, filename)
}

func (self *Server) ListenAndServe() error {
	uiDir := self.UiDirectory

	if self.UiDirectory == `embedded` {
		uiDir = `/`
	}

	if backend, err := NewDatabaseWithOptions(self.ConnectionString, self.ConnectOptions); err == nil {
		self.backend = backend
	} else {
		return err
	}

	// if specified, pre-load schema definitions
	for _, filename := range self.schemaDefs {
		if collections, err := LoadSchemataFromFile(filename); err == nil {
			log.Infof("Loaded %d definitions from %v", len(collections), filename)

			for _, collection := range collections {
				self.backend.RegisterCollection(collection)
			}
		} else {
			return err
		}
	}

	server := negroni.New()
	mux := http.NewServeMux()
	router := vestigo.NewRouter()
	ui := diecast.NewServer(uiDir, `*.html`)

	// tell diecast where loopback requests should go
	if strings.HasPrefix(self.Address, `:`) {
		ui.BindingPrefix = fmt.Sprintf("http://localhost%s", self.Address)
	} else {
		ui.BindingPrefix = fmt.Sprintf("http://%s", self.Address)
	}

	if self.UiDirectory == `embedded` {
		ui.SetFileSystem(FS(false))
	}

	if err := ui.Initialize(); err != nil {
		return err
	}

	if err := self.setupRoutes(router); err != nil {
		return err
	}

	mux.Handle(`/api/`, router)
	mux.Handle(`/`, ui)

	server.UseHandler(mux)
	// server.Use(httputil.NewRequestLogger())
	server.Run(self.Address)
	return nil
}

func (self *Server) setupRoutes(router *vestigo.Router) error {
	router.SetGlobalCors(&vestigo.CorsAccessControl{
		AllowOrigin:      []string{"*"},
		AllowCredentials: true,
		AllowMethods:     []string{`GET`, `POST`, `PUT`, `DELETE`},
		MaxAge:           3600 * time.Second,
		AllowHeaders:     []string{"*"},
	})

	router.Get(`/api/status`,
		func(w http.ResponseWriter, req *http.Request) {
			status := map[string]interface{}{
				`backend`: self.backend.GetConnectionString().String(),
			}

			if indexer := self.backend.WithSearch(``, nil); indexer != nil {
				status[`indexer`] = indexer.IndexConnectionString().String()
			}

			httputil.RespondJSON(w, status)
		})

	router.Get(`/api/collections/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)

			if collection, err := self.backend.GetCollection(name); err == nil {
				httputil.RespondJSON(w, collection)
			} else {
				httputil.RespondJSON(w, err, http.StatusNotFound)
			}
		})

	router.Get(`/api/collections/:collection/where/*urlquery`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			query := vestigo.Param(req, `_name`)

			limit := int(httputil.QInt(req, `limit`, int64(DefaultResultLimit)))
			offset := int(httputil.QInt(req, `offset`))

			if f, err := filter.Parse(query); err == nil {
				f.Limit = limit
				f.Offset = offset

				if v := httputil.Q(req, `sort`); v != `` {
					f.Sort = strings.Split(v, `,`)
				}

				if v := httputil.Q(req, `fields`); v != `` {
					f.Fields = strings.Split(v, `,`)
				}

				if search := self.backend.WithSearch(name); search != nil {
					if recordset, err := search.Query(name, f); err == nil {
						httputil.RespondJSON(w, recordset)
					} else {
						httputil.RespondJSON(w, err)
					}
				} else {
					httputil.RespondJSON(w, fmt.Errorf("Backend %T does not support complex queries.", self.backend), http.StatusBadRequest)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Get(`/api/collections/:collection/aggregate/:fields`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			fields := strings.Split(vestigo.Param(req, `fields`), `,`)
			aggregations := strings.Split(httputil.Q(req, `fn`, `count`), `,`)

			if f, err := filter.Parse(httputil.Q(req, `q`, `all`)); err == nil {

				if aggregator := self.backend.WithAggregator(name); aggregator != nil {
					results := make(map[string]interface{})

					for _, field := range fields {
						fieldResults := make(map[string]interface{})

						for _, aggregation := range aggregations {
							var value interface{}
							var err error

							switch aggregation {
							case `count`:
								value, err = aggregator.Count(name, f)
							case `sum`:
								value, err = aggregator.Sum(name, field, f)
							case `minimum`, `min`:
								value, err = aggregator.Minimum(name, field, f)
							case `maximum`, `max`:
								value, err = aggregator.Maximum(name, field, f)
							case `average`, `avg`:
								value, err = aggregator.Average(name, field, f)
							default:
								httputil.RespondJSON(w, fmt.Errorf("Unsupported aggregator %s", aggregator), http.StatusBadRequest)
								return
							}

							if err != nil {
								httputil.RespondJSON(w, err)
								return
							}

							fieldResults[aggregation] = value
						}

						results[field] = fieldResults
					}

					httputil.RespondJSON(w, results)
				} else {
					httputil.RespondJSON(w, fmt.Errorf("Backend %T does not support aggregations.", self.backend), http.StatusBadRequest)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Get(`/api/collections/:collection/list/*fields`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			fieldNames := vestigo.Param(req, `_name`)

			f := filter.All()

			if v := httputil.Q(req, `q`); v != `` {
				if fV, err := filter.Parse(v); err == nil {
					f = fV
				} else {
					httputil.RespondJSON(w, err, http.StatusBadRequest)
				}
			}

			if search := self.backend.WithSearch(name); search != nil {
				fields := strings.TrimPrefix(fieldNames, `/`)

				if recordset, err := search.ListValues(name, strings.Split(fields, `/`), f); err == nil {
					httputil.RespondJSON(w, recordset)
				} else {
					httputil.RespondJSON(w, err)
				}
			} else {
				httputil.RespondJSON(w, fmt.Errorf("Backend %T does not support complex queries.", self.backend), http.StatusBadRequest)
			}
		})

	router.Delete(`/api/collections/:collection/where/*urlquery`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			query := vestigo.Param(req, `_name`)

			if f, err := filter.Parse(query); err == nil {
				if err := self.backend.Delete(name, f); err == nil {
					httputil.RespondJSON(w, nil)
				} else {
					httputil.RespondJSON(w, err, http.StatusBadRequest)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Get(`/api/collections/:collection/records/*id`,
		func(w http.ResponseWriter, req *http.Request) {
			var id interface{}
			var fields []string
			name := vestigo.Param(req, `collection`)

			if ids := strings.Split(vestigo.Param(req, `_name`), `/`); len(ids) == 1 {
				id = ids[0]
			} else {
				id = ids
			}

			if v := httputil.Q(req, `fields`); v != `` {
				fields = strings.Split(v, `,`)
			}

			if record, err := self.backend.Retrieve(name, id, fields...); err == nil {
				httputil.RespondJSON(w, record)
			} else {
				httputil.RespondJSON(w, err)
			}
		})

	router.Post(`/api/collections/:collection/records`,
		func(w http.ResponseWriter, req *http.Request) {
			var recordset dal.RecordSet

			if err := httputil.ParseJSON(req.Body, &recordset); err == nil {
				name := vestigo.Param(req, `collection`)

				if err := self.backend.Insert(name, &recordset); err == nil {
					httputil.RespondJSON(w, &recordset)
				} else {
					httputil.RespondJSON(w, err)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Put(`/api/collections/:collection/records`,
		func(w http.ResponseWriter, req *http.Request) {
			var recordset dal.RecordSet

			if err := httputil.ParseJSON(req.Body, &recordset); err == nil {
				name := vestigo.Param(req, `collection`)

				if err := self.backend.Update(name, &recordset); err == nil {
					httputil.RespondJSON(w, nil)
				} else {
					httputil.RespondJSON(w, err)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Delete(`/api/collections/:collection/records/*id`,
		func(w http.ResponseWriter, req *http.Request) {
			var id interface{}
			name := vestigo.Param(req, `collection`)

			if ids := strings.Split(vestigo.Param(req, `_name`), `/`); len(ids) == 1 {
				id = ids[0]
			} else {
				id = ids
			}

			if err := self.backend.Delete(name, id); err == nil {
				httputil.RespondJSON(w, nil)
			} else {
				httputil.RespondJSON(w, err)
			}
		})

	router.Post(`/api/collections/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			var recordset dal.RecordSet
			name := vestigo.Param(req, `collection`)

			if err := json.NewDecoder(req.Body).Decode(&recordset); err == nil {
				if err := self.backend.Insert(name, &recordset); err == nil {
					httputil.RespondJSON(w, nil)
				} else {
					httputil.RespondJSON(w, err)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Put(`/api/collections/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			var recordset dal.RecordSet
			name := vestigo.Param(req, `collection`)

			if err := json.NewDecoder(req.Body).Decode(&recordset); err == nil {
				if err := self.backend.Update(name, &recordset); err == nil {
					httputil.RespondJSON(w, nil)
				} else {
					httputil.RespondJSON(w, err)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Get(`/api/schema`,
		func(w http.ResponseWriter, req *http.Request) {
			if names, err := self.backend.ListCollections(); err == nil {
				httputil.RespondJSON(w, names)
			} else {
				httputil.RespondJSON(w, err)
			}
		})

	router.Post(`/api/schema`,
		func(w http.ResponseWriter, req *http.Request) {
			var collection dal.Collection

			if err := json.NewDecoder(req.Body).Decode(&collection); err == nil {
				if err := self.backend.CreateCollection(&collection); err == nil {
					httputil.RespondJSON(w, collection, http.StatusCreated)

				} else if dal.IsExistError(err) {
					httputil.RespondJSON(w, err, http.StatusConflict)

				} else {
					httputil.RespondJSON(w, err)
				}

			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Get(`/api/schema/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)

			if collection, err := self.backend.GetCollection(name); err == nil {
				httputil.RespondJSON(w, collection)
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	return nil
}
