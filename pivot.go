package pivot

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/ghetzel/pivot/v3/backends"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghodss/yaml"
)

// create handy type aliases to avoid importing from all over the place
type DB = backends.Backend
type Collection = dal.Collection
type Record = dal.Record
type RecordSet = dal.RecordSet
type Filter = filter.Filter

var MonitorCheckInterval = time.Duration(10) * time.Second
var NetrcFile = ``

func NewDatabaseWithOptions(connection string, options backends.ConnectOptions) (DB, error) {
	if cs, err := dal.ParseConnectionString(connection); err == nil {
		if NetrcFile != `` {
			if err := cs.LoadCredentialsFromNetrc(NetrcFile); err != nil {
				return nil, err
			}
		}

		if backend, err := backends.MakeBackend(cs); err == nil {
			// set indexer
			if options.Indexer != `` {
				if ics, err := dal.ParseConnectionString(options.Indexer); err == nil {
					if NetrcFile != `` {
						if err := ics.LoadCredentialsFromNetrc(NetrcFile); err != nil {
							return nil, err
						}
					}

					if err := backend.SetIndexer(ics); err != nil {
						return nil, err
					}
				} else {
					return nil, err
				}
			}

			// TODO: add MultiIndexer if AdditionalIndexers is present

			if !options.SkipInitialize {
				if err := backend.Initialize(); err != nil {
					return nil, err
				}
			}

			return backend, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func NewDatabase(connection string) (DB, error) {
	return NewDatabaseWithOptions(connection, backends.ConnectOptions{})
}

func LoadSchemataFromFile(filename string) ([]*dal.Collection, error) {
	if file, err := os.Open(filename); err == nil {
		var collections []*dal.Collection

		switch ext := path.Ext(filename); ext {
		case `.json`:
			if err := json.NewDecoder(file).Decode(&collections); err != nil {
				return nil, fmt.Errorf("decode error: %v", err)
			}

		case `.yml`, `.yaml`:
			if data, err := ioutil.ReadAll(file); err == nil {
				if err := yaml.Unmarshal(data, &collections); err != nil {
					return nil, fmt.Errorf("decode error: %v", err)
				}
			} else {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("Unrecognized file extension %s", ext)
		}

		return collections, nil
	} else {
		return nil, err
	}
}
