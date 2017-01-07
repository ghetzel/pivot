package pivot

import (
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger(`pivot`)
var MonitorCheckInterval = time.Duration(10) * time.Second

func NewDatabaseWithOptions(connection string, options backends.ConnectOptions) (backends.Backend, error) {
	if cs, err := dal.ParseConnectionString(connection); err == nil {
		if backend, err := backends.MakeBackend(cs); err == nil {
			backend.SetOptions(options)

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
