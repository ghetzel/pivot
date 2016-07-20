package main

import (
	"fmt"
	"github.com/ghetzel/cli"
	"github.com/op/go-logging"
	"github.com/rs/cors"
	"github.com/urfave/negroni"
	"net/http"
	"os"
)

var log = logging.MustGetLogger(`main`)

func main() {
	app := cli.NewApp()
	app.Name = `pivot`
	app.Usage = ``
	app.Version = `0.0.1`
	app.EnableBashCompletion = false

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   `log-level, L`,
			Usage:  `Level of log output verbosity`,
			Value:  `info`,
			EnvVar: `LOGLEVEL`,
		},
		cli.StringFlag{
			Name:  `address, a`,
			Usage: `The address that the HTTP server should listen on`,
			Value: `127.0.0.1`,
		},
		cli.IntFlag{
			Name:  `port, p`,
			Usage: `The port that the HTTP server should listen on`,
			Value: 3000,
		},
	}

	app.Before = func(c *cli.Context) error {
		logging.SetFormatter(logging.MustStringFormatter(`%{color}%{level:.4s}%{color:reset}[%{id:04d}] %{message}`))

		if level, err := logging.LogLevel(c.String(`log-level`)); err == nil {
			logging.SetLevel(level, `main`)
		} else {
			return err
		}

		return nil
	}

	app.Action = func(c *cli.Context) {
		server := negroni.New()
		mux := http.NewServeMux()

		corsHandler := cors.New(cors.Options{
			AllowedOrigins:   []string{`*`},
			AllowedMethods:   []string{`GET`, `POST`},
			AllowedHeaders:   []string{`*`},
			AllowCredentials: true,
		})

		server.Use(negroni.NewRecovery())
		// server.Use(negroni.NewStatic(http.Dir("./contrib/wstest/static")))
		server.Use(corsHandler)
		server.UseHandler(mux)

		server.Run(fmt.Sprintf("%s:%d", c.String(`address`), c.Int(`port`)))
	}

	app.Run(os.Args)
}
