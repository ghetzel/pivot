package pivot

import (
	"github.com/ghetzel/pivot/backends"
)

var Backends = make(map[string]backends.IBackend)
