package main

import (
	"fmt"
	"os"

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
		cli.BoolFlag{
			Name:  `log-queries, Q`,
			Usage: `Whether to include queries in the logging output`,
		},
		cli.StringFlag{
			Name:  `config, c`,
			Usage: `Path to the configuration file to load.`,
			Value: `/etc/pivot.yml`,
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

		return nil
	}

	app.Commands = []cli.Command{
		{
			Name:      `web`,
			Usage:     `Start a web server UI.`,
			ArgsUsage: `CONNECTION_STRING`,
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
				server := pivot.NewServer(c.Args().First())
				server.Address = c.String(`address`)
				server.UiDirectory = c.String(`ui-dir`)

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
			},
			Action: func(c *cli.Context) {
				var source backends.Backend
				var destination backends.Backend

				if sourceURI := c.Args().Get(0); sourceURI != `` {
					if destinationURI := c.Args().Get(1); destinationURI != `` {
						if s, err := pivot.NewDatabase(sourceURI); err == nil {
							if err := s.Initialize(); err != nil {
								log.Fatalf("failed to initialize source: %v")
							}

							if d, err := pivot.NewDatabase(destinationURI); err == nil {
								if err := d.Initialize(); err != nil {
									log.Fatalf("failed to initialize destination: %v")
								}

								source = s
								destination = d
							} else {
								log.Fatalf("failed to connect to destination: %v")
							}
						} else {
							log.Fatalf("failed to connect to source: %v")
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

							if diffs := destCollection.Diff(collection); len(diffs) == 0 {
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
