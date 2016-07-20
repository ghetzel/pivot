package pivot

import (
	"fmt"
	"github.com/rs/cors"
	"github.com/urfave/negroni"
	"net/http"
)

const DEFAULT_SERVER_ADDRESS = `127.0.0.1`
const DEFAULT_SERVER_PORT = 3000

type Server struct {
	Address     string
	Port        int
	corsHandler *cors.Cors
	mux         *http.ServeMux
	server      *negroni.Negroni
}

func NewServer() *Server {
	return &Server{
		Address: DEFAULT_SERVER_ADDRESS,
		Port:    DEFAULT_SERVER_PORT,
	}
}

func (self *Server) ListenAndServe() error {
	self.server = negroni.New()
	self.mux = http.NewServeMux()

	self.corsHandler = cors.New(cors.Options{
		AllowedOrigins:   []string{`*`},
		AllowedMethods:   []string{`GET`, `POST`},
		AllowedHeaders:   []string{`*`},
		AllowCredentials: true,
	})

	self.server.Use(negroni.NewRecovery())
	// server.Use(negroni.NewStatic(http.Dir("./contrib/wstest/static")))
	self.server.Use(self.corsHandler)
	self.server.UseHandler(self.mux)

	self.server.Run(fmt.Sprintf("%s:%d", self.Address, self.Port))
	return nil
}
