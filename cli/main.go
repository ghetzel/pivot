package main

import (
	"github.com/ghetzel/cli"
	"github.com/ghetzel/pivot"
	"github.com/ghetzel/pivot/util"
	"github.com/op/go-logging"
	"os"
)

var log = logging.MustGetLogger(`main`)

func main() {
	app := cli.NewApp()
	app.Name = util.ApplicationName
	app.Usage = util.ApplicationSummary
	app.Version = util.ApplicationVersion
	app.EnableBashCompletion = false

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   `log-level, L`,
			Usage:  `Level of log output verbosity`,
			Value:  `info`,
			EnvVar: `LOGLEVEL`,
		},
		cli.StringFlag{
			Name:  `config, c`,
			Usage: `Path to the configuration file to load.`,
			Value: `/etc/pivot.yml`,
		},
		cli.StringFlag{
			Name:  `address, a`,
			Usage: `The local address the server should listen on.`,
			Value: pivot.DEFAULT_SERVER_ADDRESS,
		},
		cli.IntFlag{
			Name:  `port, p`,
			Usage: `The port the server should listen on.`,
			Value: pivot.DEFAULT_SERVER_PORT,
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
		server := pivot.NewServer(c.Args().First())
		server.Address = c.String(`address`)
		server.Port = c.Int(`port`)

		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
			os.Exit(3)
		}
	}

	app.Run(os.Args)
}
