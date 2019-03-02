package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/ghetzel/cli"
	"github.com/ghetzel/go-stockutil/fileutil"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/v3"
	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/client"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghetzel/pivot/v3/filter/generators"
	"github.com/ghetzel/pivot/v3/mapper"
)

func main() {
	app := cli.NewApp()
	app.Name = pivot.ApplicationName
	app.Usage = pivot.ApplicationSummary
	app.Version = pivot.ApplicationVersion
	app.EnableBashCompletion = true

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
		log.SetLevelString(c.String(`log-level`))
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

				if cnf, err := pivot.LoadConfigFile(c.GlobalString(`config`)); err == nil {
					config = cnf.ForEnv(os.Getenv(`PIVOT_ENV`))
					log.Infof("Loaded configuration file from %v env=%v", c.GlobalString(`config`), os.Getenv(`PIVOT_ENV`))
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

				if backend == `` {
					log.Fatalf("Must specify a backend to connect to.")
				}

				server := pivot.NewServer(backend)
				server.Address = c.String(`address`)
				server.UiDirectory = c.String(`ui-dir`)
				server.ConnectOptions.Indexer = indexer
				server.Autoexpand = config.Autoexpand

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
					if collection, err := source.GetCollection(name); err == nil {
						if indexer := source.WithSearch(collection); indexer != nil {
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
							log.Errorf("Cannot export source collection %q: collection is not enumerable", name)
						}
					} else {
						log.Errorf("Cannot export source collection %q: %v", name, err)
					}
				}
			},
		}, {
			Name:      `filter`,
			Usage:     `Converts a given filter into the specified native query`,
			ArgsUsage: `COLLECTION GENERATOR[:SUBTYPE] FILTER`,
			Action: func(c *cli.Context) {
				col := c.Args().Get(0)
				gentype, sub := stringutil.SplitPair(c.Args().Get(1), `:`)
				flt := c.Args().Get(2)

				if f, err := filter.Parse(flt); err == nil {
					var gen filter.IGenerator

					switch gentype {
					case `sql`:
						g := generators.NewSqlGenerator()

						if mapping, err := generators.GetSqlTypeMapping(sub); err == nil {
							g.TypeMapping = mapping
						} else {
							log.Fatal(err)
						}

						gen = g

					case `mongodb`:
						gen = generators.NewMongoDBGenerator()

					case `elasticsearch`, `es`:
						gen = generators.NewElasticsearchGenerator()

					default:
						log.Fatalf("Unrecognized query generator %q", gentype)
					}

					if data, err := filter.Render(gen, col, f); err == nil {
						fmt.Println(string(data))
					} else {
						log.Fatalf("error generating query: %v", err)
					}
				} else {
					log.Fatalf("invalid filter: %v", err)
				}
			},
		}, {
			Name:  `client`,
			Usage: `Provides an HTTP API client for interacting with a running Pivot instance.`,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   `url, u`,
					Usage:  `The Pivot API URL to connect to.`,
					EnvVar: `PIVOT_URL`,
					Value:  client.DefaultPivotUrl,
				},
				cli.StringFlag{
					Name:  `format, f`,
					Usage: `How to format API result output. (one of: text, json, yaml)`,
				},
				cli.BoolFlag{
					Name:  `pretty, P`,
					Usage: `Pretty-print the formatted output (indenting where applicable)`,
				},
			},
			Subcommands: cli.Commands{
				{
					Name:  `status`,
					Usage: `Retrieve status info from Pivot.`,
					Action: func(c *cli.Context) {
						if status, err := pivotClient(c).Status(); err == nil {
							output(c, status, func() error {
								fmt.Printf(
									"%v %v: backend=%v indexer=%v\n",
									status.Application,
									status.Version,
									status.Backend,
									status.Indexer,
								)

								return nil
							})
						} else {
							log.Fatal(err)
						}
					},
				}, {
					Name:      `collections`,
					Usage:     `Return a list of collection names or details on a specific collection.`,
					ArgsUsage: `[NAME]`,
					Action: func(c *cli.Context) {
						if specific := c.Args(); len(specific) == 0 {
							if names, err := pivotClient(c).Collections(); err == nil {
								output(c, names, func() error {
									if len(names) > 0 {
										fmt.Println(strings.Join(names, "\n"))
									}

									return nil
								})
							} else {
								log.Fatal(err)
							}
						} else {
							for _, name := range specific {
								if collection, err := pivotClient(c).Collection(name); err == nil {
									output(c, collection, nil)
								} else {
									log.Fatal(err)
								}
							}
						}
					},
				}, {
					Name: `records`,
					Usage: `Retrieve (or create, update) records from a specific collection. ` +
						`If data is read from standard input, it will be inserted/updated into the ` +
						`collection specified in the Record's "collection" field, falling back to ` +
						`the collection specified in the first positional argument after this subcommand.`,
					ArgsUsage: `[COLLECTION [ID ..]]`,
					Flags: []cli.Flag{
						cli.BoolFlag{
							Name:  `warn-errors, w`,
							Usage: `Errors creating/updating records will only produce warnings instead of exiting the program.`,
						},
					},
					Action: func(c *cli.Context) {
						pc := pivotClient(c)
						collection := c.Args().First()
						ids := make([]string, 0)

						if args := c.Args(); len(args) > 1 {
							ids = args[1:]
						}

						// if stdin is not a terminal (e.g.: something is being piped into the command)
						// then read and parse lines as JSON dal.Records
						if !fileutil.IsTerminal() {
							lines := bufio.NewScanner(os.Stdin)

							for lines.Scan() {
								line := strings.TrimSpace(lines.Text())

								if line == `` || strings.HasPrefix(line, `#`) {
									continue
								}

								var record dal.Record

								if err := json.Unmarshal([]byte(line), &record); err == nil {

									if record.CollectionName != `` {
										collection = record.CollectionName
									}

									if collection == `` {
										logerr(c, "No collection specified in record or on command line")
									}

									if record.Operation == `` {
										if record.ID == nil {
											record.Operation = `create`
										} else {
											record.Operation = `update`
										}
									}

									switch record.Operation {
									case `create`:
										_, err = pc.CreateRecord(collection, &record)
									case `update`:
										_, err = pc.UpdateRecord(collection, &record)
									case `delete`:
										err = pc.DeleteRecords(collection, record.ID)
									default:
										logerr(c, "Unknown record operation %q", record.Operation)
									}
								} else {
									logerr(c, "malformed input: %v", err)
								}
							}

							if err := lines.Err(); err != nil {
								logerr(c, "error reading input: %v", err)
							}
						}

						if len(ids) > 0 {
							if collection == `` {
								log.Fatalf("Must specify a collection to retrieve records from.")
							}

							// read any records explicitly requested in positional arguments
							// this can be cleverly used as a readback for records just written via stdin
							for _, id := range ids {
								if record, err := pc.GetRecord(collection, id); err == nil {
									output(c, record, nil)
								} else {
									logerr(c, "retrieve error: %v", err)
								}
							}
						}
					},
				}, {
					Name:      `query`,
					Usage:     `Query a collection and output the results.`,
					ArgsUsage: `COLLECTION [FILTERS ..]`,
					Flags: []cli.Flag{
						cli.IntFlag{
							Name:  `limit, l`,
							Usage: `Limit the number of records to return in one query`,
						},
						cli.IntFlag{
							Name:  `offset, o`,
							Usage: `The record offset to apply to the query (for retrieving paginated results)`,
						},
						cli.StringFlag{
							Name:  `sort, s`,
							Usage: `A comma-separated list of fields to sort the results by (in order of priority)`,
						},
						cli.StringFlag{
							Name:  `fields, f`,
							Usage: `A comma-separated list of fields to include in the resulting records.`,
						},
						cli.BoolFlag{
							Name:  `autopage, P`,
							Usage: `Automatically paginate through results, performing the query`,
						},
					},
					Action: func(c *cli.Context) {
						if collection := c.Args().First(); collection != `` {
							filters := make([]string, 0)
							offset := c.Int(`offset`)
							fSort := sliceutil.CompactString(strings.Split(c.String(`sort`), `,`))
							fFields := sliceutil.CompactString(strings.Split(c.String(`fields`), `,`))

							if args := c.Args(); len(args) > 1 {
								filters = args[1:]
							}

							for {
								if results, err := pivotClient(c).Query(collection, filters, &client.QueryOptions{
									Limit:  c.Int(`limit`),
									Offset: offset,
									Sort:   fSort,
									Fields: fFields,
								}); err == nil {
									for _, record := range results.Records {
										output(c, record.Map(fFields...), nil)
									}

									if len(results.Records) == 0 {
										return
									} else if !c.Bool(`autopage`) {
										return
									}

									offset += len(results.Records)
								} else {
									log.Fatal(err)
									return
								}
							}
						} else {
							log.Fatalf("Must specify a collection to query.")
						}
					},
				},
			},
		},
	}

	app.Run(os.Args)
}

func logerr(c *cli.Context, format string, args ...interface{}) {
	if c.Bool(`warn-errors`) {
		log.Warningf(format, args...)
	} else {
		log.Fatalf(format, args...)
	}
}

func pivotClient(c *cli.Context) *client.Pivot {
	if c, err := client.New(c.String(`url`)); err == nil {
		return c
	} else {
		log.Fatalf("client error: %v", err)
		return nil
	}
}

func output(c *cli.Context, value interface{}, textFn func() error) error {
	format := c.String(`format`)

	var err error

	if textFn == nil {
		format = `json`
	}

	switch format {
	case `json`:
		enc := json.NewEncoder(os.Stdout)

		if c.Bool(`pretty`) {
			enc.SetIndent(``, `  `)
		}

		err = enc.Encode(value)
	default:
		if textFn != nil {
			err = textFn()
		}
	}

	return err
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
