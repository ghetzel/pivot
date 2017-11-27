package pivot

import (
	"time"

	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger(`pivot`)
var MonitorCheckInterval = time.Duration(10) * time.Second

func NewDatabaseWithOptions(connection string, options backends.ConnectOptions) (backends.Backend, error) {
	if cs, err := dal.ParseConnectionString(connection); err == nil {
		if backend, err := backends.MakeBackend(cs); err == nil {
			// set indexer
			if options.Indexer != `` {
				if ics, err := dal.ParseConnectionString(options.Indexer); err == nil {
					if err := backend.SetIndexer(ics); err != nil {
						return nil, err
					}
				} else {
					return nil, err
				}
			}

			// TODO: add MultiIndexer if AdditionalIndexers is present

			if err := backend.Initialize(); err == nil {
				return backend, nil
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func NewDatabase(connection string) (backends.Backend, error) {
	return NewDatabaseWithOptions(connection, backends.ConnectOptions{})
}
