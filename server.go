package pivot

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/pivot/util"
	"github.com/julienschmidt/httprouter"
	"github.com/op/go-logging"
	"github.com/rs/cors"
	"github.com/urfave/negroni"
	"net/http"
	"time"
)

var log = logging.MustGetLogger(`pivot`)

const DEFAULT_SERVER_ADDRESS = `127.0.0.1`
const DEFAULT_SERVER_PORT = 29029

type Server struct {
	Address     string
	Port        int
	corsHandler *cors.Cors
	router      *httprouter.Router
	server      *negroni.Negroni
	endpoints   []util.Endpoint
	routeMap    map[string]util.EndpointResponseFunc
}

func NewServer() *Server {
	return &Server{
		Address:   DEFAULT_SERVER_ADDRESS,
		Port:      DEFAULT_SERVER_PORT,
		endpoints: make([]util.Endpoint, 0),
		routeMap:  make(map[string]util.EndpointResponseFunc),
	}
}

func (self *Server) ListenAndServe() error {
	self.server = negroni.New()
	self.router = httprouter.New()

	self.corsHandler = cors.New(cors.Options{
		AllowedOrigins:   []string{`*`},
		AllowedMethods:   []string{`GET`, `POST`},
		AllowedHeaders:   []string{`*`},
		AllowCredentials: true,
	})

	self.server.Use(negroni.NewRecovery())
	// server.Use(negroni.NewStatic(http.Dir("./contrib/wstest/static")))
	self.server.Use(self.corsHandler)
	self.server.UseHandler(self.router)

	if err := self.setupBackendRoutes(); err != nil {
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
