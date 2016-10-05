package pivot

import (
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger(`pivot`)
var MonitorCheckInterval = time.Duration(10) * time.Second
