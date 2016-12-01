package pivot

import (
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger(`pivot`)
var MonitorCheckInterval = time.Duration(10) * time.Second

func NewDatabase(connection string) (backends.Backend, error) {
	if cs, err := dal.ParseConnectionString(connection); err == nil {
		if backend, err := backends.MakeBackend(cs); err == nil {
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
