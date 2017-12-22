package main

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"

	"github.com/ghetzel/cli"
	"github.com/ghetzel/pivot"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/mapper"
	"github.com/ghetzel/pivot/util"
	"github.com/op/go-logging"
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
			Value:  `debug`,
			EnvVar: `LOGLEVEL`,
		},
		cli.StringFlag{
			Name:  `config, c`,
			Usage: `Path to the configuration file to load.`,
			Value: `pivot.yml`,
		},
		cli.BoolFlag{
			Name:  `log-queries, Q`,
			Usage: `Whether to include queries in the logging output`,
		},
		cli.StringSliceFlag{
			Name:  `schema, s`,
			Usage: `Path to one or more schema files to load`,
		},
		cli.BoolTFlag{
			Name:  `allow-netrc, N`,
			Usage: `Allow parsing of a .netrc file.`,
		},
		cli.StringFlag{
			Name:  `netrc, n`,
			Usage: `Specify the location of the .netrc file to parse.`,
		},
	}

	app.Before = func(c *cli.Context) error {
		logging.SetFormatter(logging.MustStringFormatter(`%{color}%{level:.4s}%{color:reset}[%{id:04d}] %{message}`))

		if level, err := logging.LogLevel(c.String(`log-level`)); err == nil {
			logging.SetLevel(level, ``)
		} else {
			return err
		}

		if c.Bool(`log-queries`) {
			logging.SetLevel(logging.DEBUG, `pivot/querylog`)
		} else {
			logging.SetLevel(logging.CRITICAL, `pivot/querylog`)
		}

		populateNetrc(c)

		return nil
	}

	app.Commands = []cli.Command{
		{
			Name:      `web`,
			Usage:     `Start a web server UI.`,
			ArgsUsage: `[CONNECTION_STRING [INDEXER_CONNECTION_STRING]]`,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  `address, a`,
					Usage: `The local address the server should listen on.`,
					Value: fmt.Sprintf("%s:%d", pivot.DefaultAddress, pivot.DefaultPort),
				},
				cli.StringFlag{
					Name:  `ui-dir`,
					Usage: `The path to the UI directory`,
					Value: pivot.DefaultUiDirectory,
				},
			},
			Action: func(c *cli.Context) {
				var backend string
				var indexer string
				var config pivot.Configuration

				if c, err := pivot.LoadConfigFile(c.GlobalString(`config`)); err == nil {
					config = c.ForEnv(os.Getenv(`PIVOT_ENV`))
				} else if !os.IsNotExist(err) {
					log.Fatalf("Configuration error: %v", err)
				}

				if v := c.Args().Get(0); v != `` {
					backend = v
				} else {
					backend = config.Backend
				}

				if v := c.Args().Get(1); v != `` {
					indexer = v
				} else {
					indexer = config.Indexer
				}

				server := pivot.NewServer(backend)
				server.Address = c.String(`address`)
				server.UiDirectory = c.String(`ui-dir`)
				server.ConnectOptions.Indexer = indexer

				for _, filename := range c.GlobalStringSlice(`schema`) {
					server.AddSchemaDefinition(filename)
				}

				if err := server.ListenAndServe(); err != nil {
					log.Fatalf("Failed to start server: %v", err)
					os.Exit(3)
				}
			},
		},
		{
			Name:      `copy`,
			Usage:     `Copies data from one datasource to another`,
			ArgsUsage: `SOURCE DESTINATION`,
			Flags: []cli.Flag{
				cli.StringSliceFlag{
					Name:  `collection, c`,
					Usage: `A specific collection to copy (can be specified multiple times).`,
				},
				cli.BoolFlag{
					Name:  `no-schema-check, S`,
					Usage: `Skip verifying schema equality.`,
				},
			},
			Action: func(c *cli.Context) {
				var source backends.Backend
				var destination backends.Backend

				if sourceURI := c.Args().Get(0); sourceURI != `` {
					if destinationURI := c.Args().Get(1); destinationURI != `` {
						if s, err := pivot.NewDatabase(sourceURI); err == nil {
							if err := s.Initialize(); err != nil {
								log.Fatalf("failed to initialize source: %v", err)
							}

							if d, err := pivot.NewDatabase(destinationURI); err == nil {
								if err := d.Initialize(); err != nil {
									log.Fatalf("failed to initialize destination: %v", err)
								}

								source = s
								destination = d
							} else {
								log.Fatalf("failed to connect to destination: %v", err)
							}
						} else {
							log.Fatalf("failed to connect to source: %v", err)
						}
					} else {
						log.Fatalf("Must specify a destination")
					}
				} else {
					log.Fatalf("Must specify a source")
				}

				collections := c.StringSlice(`collection`)

				if len(collections) == 0 {
					if c, err := source.ListCollections(); err == nil {
						collections = c
					} else {
						log.Fatalf("failed to list source collections: %v", err)
					}
				}

				log.Debugf("Copying %d collections", len(collections))

				for _, name := range collections {
					if indexer := source.WithSearch(name); indexer != nil {
						if collection, err := source.GetCollection(name); err == nil {
							var destCollection *dal.Collection

							if dc, err := destination.GetCollection(name); err == nil {
								destCollection = dc

							} else if dal.IsCollectionNotFoundErr(err) {
								if err := destination.CreateCollection(collection); err == nil {
									destCollection = collection
								} else {
									log.Errorf("Cannot create destination collection %q: %v", name, err)
									continue
								}
							} else {
								log.Errorf("Cannot import to destination collection %q: %v", name, err)
								continue
							}

							if diffs := destCollection.Diff(collection); len(diffs) == 0 || c.Bool(`no-schema-check`) {
								sourceItem := mapper.NewModel(source, collection)
								var i int

								if err := sourceItem.Each(&dal.Record{}, func(ptrToInstance interface{}, err error) {
									if newRecord, ok := ptrToInstance.(*dal.Record); ok && err == nil {
										if err := destination.Insert(name, dal.NewRecordSet(newRecord)); err == nil {
											i += 1
											log.Debugf("Copied record %v", newRecord.ID)
										} else {
											log.Warningf("failed to write record to destination: %v", err)
										}
									} else if !ok {
										log.Warningf("failed to copy record: invalid return type %T", ptrToInstance)
									} else {
										log.Warningf("failed to copy record: %v", i, err)
									}
								}); err == nil {
									log.Noticef("Successfully copied %d records from collection %q", i, name)
								} else {
									log.Errorf("Failed to copy collection %q: %v", name, err)
								}
							} else {
								log.Errorf("Cannot import to destination collection %q: collections differ", name)

								for _, diff := range diffs {
									log.Errorf("  %v", diff)
								}
							}
						} else {
							log.Errorf("Cannot export source collection %q: %v", name, err)
						}
					} else {
						log.Errorf("Cannot export source collection %q: collection is not enumerable", name)
					}
				}
			},
		},
	}

	app.Run(os.Args)
}

func populateNetrc(c *cli.Context) {
	if netrc := c.String(`netrc`); c.Bool(`allow-netrc`) {
		if netrc == `` {
			if usr, err := user.Current(); err == nil {
				pivot.NetrcFile = filepath.Join(usr.HomeDir, ".netrc")
			} else {
				log.Fatalf("netrc err: %v", err)
			}
		} else {
			pivot.NetrcFile = netrc
		}
	} else {
		pivot.NetrcFile = ``
	}
}
