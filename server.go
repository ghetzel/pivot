package pivot

//go:generate esc -o static.go -pkg pivot -modtime 1500000000 -prefix ui ui

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ghetzel/diecast"
	"github.com/ghetzel/go-stockutil/fileutil"
	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghetzel/pivot/v3/util"
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
	Autoexpand       bool
	backend          DB
	endpoints        []util.Endpoint
	routeMap         map[string]util.EndpointResponseFunc
	schemaDefs       []string
	fixturePaths     []string
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

func (self *Server) AddSchemaDefinition(fileOrDirPath string) {
	if pathutil.DirExists(fileOrDirPath) {
		if entries, err := ioutil.ReadDir(fileOrDirPath); err == nil {
			for _, entry := range entries {
				if entry.Mode().IsRegular() {
					self.schemaDefs = append(self.schemaDefs, path.Join(fileOrDirPath, entry.Name()))
				}
			}
		}
	} else if pathutil.FileExists(fileOrDirPath) {
		self.schemaDefs = append(self.schemaDefs, fileOrDirPath)
	}
}

func (self *Server) AddFixturePath(fileOrDirPath string) {
	self.fixturePaths = append(self.fixturePaths, fileOrDirPath)
}

func (self *Server) ListenAndServe() error {
	uiDir := self.UiDirectory
	loadedCollections := make([]*dal.Collection, 0)

	if d := os.Getenv(`UI`); fileutil.DirExists(d) {
		uiDir = d
	} else if self.UiDirectory == `embedded` {
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
				loadedCollections = append(loadedCollections, collection)
			}
		} else {
			return err
		}
	}

	// autocreate the collections (if specified)
	if self.ConnectOptions.AutocreateCollections {
		for _, schema := range loadedCollections {
			if _, err := self.backend.GetCollection(schema.Name); err == nil {
				continue
			} else if dal.IsCollectionNotFoundErr(err) {
				if err := self.backend.CreateCollection(schema); err == nil {
					log.Noticef("[%v] Created collection %q", self.backend, schema.Name)
				} else {
					log.Errorf("[%v] Error creating collection %q: %v", self.backend, schema.Name, err)
				}
			} else {
				return fmt.Errorf("error creating collection %q: %v", schema.Name, err)
			}
		}
	}

	// load fixtures (if provided)
	for _, filename := range self.fixturePaths {
		if err := LoadFixtures(filename, self.backend); err != nil {
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
	server.Use(httputil.NewRequestLogger())
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
			backend := backendForRequest(self, req, self.backend)
			status := util.Status{
				OK:          true,
				Application: ApplicationName,
				Version:     ApplicationVersion,
				Backend:     backend.GetConnectionString().String(),
			}

			if indexer := backend.WithSearch(nil, nil); indexer != nil {
				status.Indexer = indexer.IndexConnectionString().String()
			}

			httputil.RespondJSON(w, &status)
		})

	// Querying
	// ---------------------------------------------------------------------------------------------
	queryHandler := func(w http.ResponseWriter, req *http.Request) {
		var query interface{}
		var name string
		var leftField string
		var rightName string
		var rightField string

		collections := strings.Split(vestigo.Param(req, `collection`), `:`)

		switch len(collections) {
		case 1:
			name = collections[0]
		case 2:
			name, leftField = stringutil.SplitPair(collections[0], `.`)
			rightName, rightField = stringutil.SplitPair(collections[1], `.`)
		default:
			httputil.RespondJSON(w, fmt.Errorf("Only two (2) joined collections are supported"), http.StatusBadRequest)
			return
		}

		switch req.Method {
		case `GET`:
			if q := vestigo.Param(req, `_name`); q != `` {
				query = q
			}

		case `POST`:
			fMap := make(map[string]interface{})

			if err := httputil.ParseRequest(req, &fMap); err == nil {
				query = fMap
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
				return
			}
		}

		backend := backendForRequest(self, req, self.backend)

		if f, err := filterFromRequest(req, query, int64(DefaultResultLimit)); err == nil {
			if collection, err := backend.GetCollection(name); err == nil {
				collection = injectRequestParamsIntoCollection(req, collection)

				var queryInterface backends.Indexer

				if search := backend.WithSearch(collection, f); search != nil {
					if rightName == `` {
						queryInterface = search
					} else {
						if rightCollection, err := backend.GetCollection(rightName); err == nil {
							// leaving this here, though a little redundant, for when we support heterogeneous backends
							if rightSearch := backend.WithSearch(rightCollection, f); rightSearch != nil {
								queryInterface = backends.NewMetaIndex(
									search,
									collection,
									leftField,
									rightSearch,
									rightCollection,
									rightField,
								)
							} else {
								httputil.RespondJSON(w, fmt.Errorf("Backend %T does not support complex queries.", self.backend), http.StatusBadRequest)
								return
							}
						} else {
							httputil.RespondJSON(w, fmt.Errorf("right-side: %v", err))
							return
						}
					}

					if recordset, err := queryInterface.Query(collection, f); err == nil {
						httputil.RespondJSON(w, recordset)
					} else {
						httputil.RespondJSON(w, err)
					}
				} else {
					httputil.RespondJSON(w, fmt.Errorf("Backend %T does not support complex queries.", self.backend), http.StatusBadRequest)
				}
			} else if dal.IsCollectionNotFoundErr(err) {
				httputil.RespondJSON(w, err, http.StatusNotFound)
			} else {
				httputil.RespondJSON(w, err)
			}
		} else {
			httputil.RespondJSON(w, err, http.StatusBadRequest)
		}
	}

	router.Post(`/api/collections/:collection/query/`, queryHandler)
	router.Get(`/api/collections/:collection/query/`, queryHandler)
	router.Get(`/api/collections/:collection/where/*urlquery`, queryHandler)

	router.Get(`/api/collections/:collection/aggregate/:fields`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			fields := strings.Split(vestigo.Param(req, `fields`), `,`)
			aggregations := strings.Split(httputil.Q(req, `fn`, `count`), `,`)
			backend := backendForRequest(self, req, self.backend)

			if f, err := filterFromRequest(req, httputil.Q(req, `q`, `all`), 0); err == nil {
				if collection, err := backend.GetCollection(name); err == nil {
					collection = injectRequestParamsIntoCollection(req, collection)

					if aggregator := backend.WithAggregator(collection); aggregator != nil {
						results := make(map[string]interface{})

						for _, field := range fields {
							fieldResults := make(map[string]interface{})

							for _, aggregation := range aggregations {
								var value interface{}
								var err error

								switch aggregation {
								case `count`:
									value, err = aggregator.Count(collection, f)
								case `sum`:
									value, err = aggregator.Sum(collection, field, f)
								case `min`:
									value, err = aggregator.Minimum(collection, field, f)
								case `max`:
									value, err = aggregator.Maximum(collection, field, f)
								case `avg`:
									value, err = aggregator.Average(collection, field, f)
								default:
									httputil.RespondJSON(w, fmt.Errorf("Unsupported aggregator '%s'", aggregation), http.StatusBadRequest)
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
				} else if dal.IsCollectionNotFoundErr(err) {
					httputil.RespondJSON(w, err, http.StatusNotFound)
				} else {
					httputil.RespondJSON(w, err)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Get(`/api/collections/:collection/list/*fields`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			fieldNames := vestigo.Param(req, `_name`)
			backend := backendForRequest(self, req, self.backend)

			if f, err := filterFromRequest(req, httputil.Q(req, `q`, `all`), 0); err == nil {
				if collection, err := backend.GetCollection(name); err == nil {
					collection = injectRequestParamsIntoCollection(req, collection)

					if search := backend.WithSearch(collection); search != nil {
						fields := strings.TrimPrefix(fieldNames, `/`)

						if recordset, err := search.ListValues(collection, strings.Split(fields, `/`), f); err == nil {
							httputil.RespondJSON(w, recordset)
						} else {
							httputil.RespondJSON(w, err)
						}
					} else {
						httputil.RespondJSON(w, fmt.Errorf("Backend %T does not support complex queries.", self.backend), http.StatusBadRequest)
					}
				} else if dal.IsCollectionNotFoundErr(err) {
					httputil.RespondJSON(w, err, http.StatusNotFound)
				} else {
					httputil.RespondJSON(w, err)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Delete(`/api/collections/:collection/where/*urlquery`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			query := vestigo.Param(req, `_name`)
			backend := backendForRequest(self, req, self.backend)

			if collection, err := backend.GetCollection(name); err == nil {
				if search := backend.WithSearch(collection); search != nil {
					if f, err := filter.Parse(query); err == nil {
						if err := search.DeleteQuery(collection, f); err == nil {
							httputil.RespondJSON(w, nil)
						} else {
							httputil.RespondJSON(w, fmt.Errorf("delete error: %v", err), http.StatusBadRequest)
						}
					} else {
						httputil.RespondJSON(w, fmt.Errorf("filter error: %v", err), http.StatusBadRequest)
					}
				} else {
					httputil.RespondJSON(w, fmt.Errorf("index error: backend does not support querying"), http.StatusBadRequest)
				}
			} else {
				httputil.RespondJSON(w, fmt.Errorf("collection error: %v", err), http.StatusBadRequest)
			}
		})

	// Record CRUD
	// ---------------------------------------------------------------------------------------------

	recordUpsert := func(w http.ResponseWriter, req *http.Request) {
		var recordset dal.RecordSet

		backend := backendForRequest(self, req, self.backend)

		if err := httputil.ParseRequest(req, &recordset); err == nil {
			if dchar := httputil.Q(req, `diffuse`); dchar != `` {
				for _, record := range recordset.Records {
					if diffused, err := maputil.DiffuseMap(record.Fields, dchar); err == nil {
						record.Fields = diffused
					} else {
						httputil.RespondJSON(w, err, http.StatusBadRequest)
						return
					}
				}
			}

			name := vestigo.Param(req, `collection`)
			status := http.StatusAccepted

			var err error

			if req.Method == `PUT` || httputil.QBool(req, `update`) {
				err = backend.Update(name, &recordset)
			} else {
				err = backend.Insert(name, &recordset)
				status = http.StatusCreated
			}

			if err == nil {
				if redirect := httputil.Q(req, `redirect`); strings.HasPrefix(redirect, `/`) {
					http.Redirect(w, req, redirect, http.StatusTemporaryRedirect)
				} else {
					httputil.RespondJSON(w, recordset, status)
				}
			} else {
				httputil.RespondJSON(w, err)
			}
		} else {
			httputil.RespondJSON(w, err, http.StatusBadRequest)
		}
	}

	router.Post(`/api/collections/:collection/records`, recordUpsert)
	router.Put(`/api/collections/:collection/records`, recordUpsert)

	router.Get(`/api/collections/:collection/records/:id`,
		func(w http.ResponseWriter, req *http.Request) {
			var id interface{}
			var fields []string

			name := vestigo.Param(req, `collection`)
			backend := backendForRequest(self, req, self.backend)

			if ids := strings.Split(vestigo.Param(req, `id`), `:`); len(ids) == 1 {
				id = ids[0]
			} else {
				id = ids
			}

			if v := httputil.Q(req, `fields`); v != `` {
				fields = strings.Split(v, `,`)
			}

			if record, err := backend.Retrieve(name, id, fields...); err == nil {
				httputil.RespondJSON(w, record)
			} else if strings.HasSuffix(err.Error(), `does not exist`) {
				httputil.RespondJSON(w, err, http.StatusNotFound)
			} else {
				httputil.RespondJSON(w, err)
			}
		})

	router.Post(`/api/collections/:collection/records/:id`,
		func(w http.ResponseWriter, req *http.Request) {
			var record dal.Record

			backend := backendForRequest(self, req, self.backend)

			if err := httputil.ParseRequest(req, &record); err == nil {
				recordset := dal.NewRecordSet(&record)
				name := vestigo.Param(req, `collection`)
				var err error

				if backend.Exists(name, record.ID) {
					err = backend.Update(name, recordset)
				} else {
					err = backend.Insert(name, recordset)
				}

				if err == nil {
					httputil.RespondJSON(w, &record)
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
			backend := backendForRequest(self, req, self.backend)

			if ids := strings.Split(vestigo.Param(req, `_name`), `/`); len(ids) == 1 {
				id = ids[0]
			} else {
				id = ids
			}

			if err := backend.Delete(name, id); err == nil {
				httputil.RespondJSON(w, nil)
			} else {
				httputil.RespondJSON(w, err)
			}
		})

	// Schema Operations
	// ---------------------------------------------------------------------------------------------
	router.Get(`/api/collections/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			backend := backendForRequest(self, req, self.backend)

			if collection, err := backend.GetCollection(name); err == nil {
				collection = injectRequestParamsIntoCollection(req, collection)

				httputil.RespondJSON(w, collection)
			} else {
				httputil.RespondJSON(w, err, http.StatusNotFound)
			}
		})

	router.Post(`/api/collections/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			var recordset dal.RecordSet

			name := vestigo.Param(req, `collection`)
			backend := backendForRequest(self, req, self.backend)

			if err := httputil.ParseRequest(req, &recordset); err == nil {
				if err := backend.Insert(name, &recordset); err == nil {
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
			backend := backendForRequest(self, req, self.backend)

			if err := httputil.ParseRequest(req, &recordset); err == nil {
				if err := backend.Update(name, &recordset); err == nil {
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
			backend := backendForRequest(self, req, self.backend)

			if names, err := backend.ListCollections(); err == nil {
				httputil.RespondJSON(w, names)
			} else {
				httputil.RespondJSON(w, err)
			}
		})

	router.Post(`/api/schema`,
		func(w http.ResponseWriter, req *http.Request) {
			var collections []dal.Collection

			backend := backendForRequest(self, req, self.backend)

			if body, err := ioutil.ReadAll(req.Body); err == nil {
				var collection dal.Collection

				if err := json.Unmarshal(body, &collection); err == nil {
					collections = append(collections, collection)
				} else if strings.Contains(err.Error(), `cannot unmarshal array `) {
					if err := json.Unmarshal(body, &collections); err != nil {
						httputil.RespondJSON(w, err, http.StatusBadRequest)
						return
					}
				} else {
					httputil.RespondJSON(w, err, http.StatusBadRequest)
				}
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
				return
			}

			var errors []error

			for _, collection := range collections {
				if err := backend.CreateCollection(&collection); err == nil {
					httputil.RespondJSON(w, collection, http.StatusCreated)

				} else if len(collections) == 1 {
					if dal.IsExistError(err) {
						httputil.RespondJSON(w, err, http.StatusConflict)
					} else {
						httputil.RespondJSON(w, err)
					}

					return
				} else {
					errors = append(errors, err)
				}
			}

			if len(errors) > 0 {
				httputil.RespondJSON(w, errors, http.StatusBadRequest)
			}
		})

	router.Get(`/api/schema/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			backend := backendForRequest(self, req, self.backend)

			if collection, err := backend.GetCollection(name); err == nil {
				collection = injectRequestParamsIntoCollection(req, collection)

				httputil.RespondJSON(w, collection)
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	router.Delete(`/api/schema/:collection`,
		func(w http.ResponseWriter, req *http.Request) {
			name := vestigo.Param(req, `collection`)
			backend := backendForRequest(self, req, self.backend)

			if err := backend.DeleteCollection(name); err == nil {
				httputil.RespondJSON(w, nil)
			} else {
				httputil.RespondJSON(w, err, http.StatusBadRequest)
			}
		})

	return nil
}

func injectRequestParamsIntoCollection(req *http.Request, collection *dal.Collection) *dal.Collection {
	// shallow copy the collection so we can screw with it
	c := *collection
	collection = &c

	if v := httputil.Q(req, `index`); v != `` {
		collection.IndexName = v
	}

	if v := httputil.Q(req, `keys`); v != `` {
		collection.IndexCompoundFields = strings.Split(v, `,`)
	}

	if v := httputil.Q(req, `joiner`); v != `` {
		collection.IndexCompoundFieldJoiner = v
	}

	return collection
}

func filterFromRequest(req *http.Request, filterIn interface{}, defaultLimit int64) (*filter.Filter, error) {
	limit := int(httputil.QInt(req, `limit`, defaultLimit))
	offset := int(httputil.QInt(req, `offset`))
	var f *filter.Filter

	switch filterIn.(type) {
	case string:
		if flt, err := filter.Parse(filterIn.(string)); err == nil {
			f = flt
		} else {
			return nil, err
		}
	case *filter.Filter:
		f = filterIn.(*filter.Filter)

	default:
		if typeutil.IsMap(filterIn) {
			if fMap, err := maputil.Compact(maputil.Autotype(filterIn)); err == nil {
				if flt, err := filter.FromMap(fMap); err == nil {
					f = flt
				} else {
					return nil, fmt.Errorf("filter parse error: %v", err)
				}
			} else {
				return nil, fmt.Errorf("map error: %v", err)
			}
		} else {
			return nil, fmt.Errorf("Unsupported filter input type %T", filterIn)
		}
	}

	f.Limit = limit
	f.Offset = offset

	if v := httputil.Q(req, `sort`); v != `` {
		f.Sort = strings.Split(v, `,`)
	}

	if v := httputil.Q(req, `fields`); v != `` {
		f.Fields = strings.Split(v, `,`)
	}

	if v := httputil.Q(req, `conjunction`); v != `` {
		switch v {
		case `and`:
			f.Conjunction = filter.AndConjunction
		case `or`:
			f.Conjunction = filter.OrConjunction
		default:
			return nil, fmt.Errorf("Unsupported conjunction operator '%s'", v)
		}
	}

	return f, nil
}

func backendForRequest(server *Server, req *http.Request, backend DB) DB {
	nx := httputil.Q(req, `noexpand`)
	skipKeys := make([]string, 0)
	useEmbeddedBackend := server.Autoexpand

	// if the ?noexpand querystring was provided in some form...
	if nx != `` {
		// noexpand=true means "don't wrap the backend in the embedded dingus at all"
		if stringutil.IsBooleanTrue(nx) {
			useEmbeddedBackend = false
		} else if !stringutil.IsBooleanFalse(nx) {
			// noexpand=<anything other than a falsey value> means "wrap, but skip these fields when expanding"
			skipKeys = httputil.QStrings(req, `noexpand`, `,`)
			useEmbeddedBackend = true
		} else {
			// noexpand=false means "wrap and expand all fields"
			useEmbeddedBackend = true
		}
	}

	if useEmbeddedBackend {
		backend = backends.NewEmbeddedRecordBackend(backend, skipKeys...)
	}

	return backend
}
