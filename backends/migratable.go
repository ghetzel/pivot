package backends

import (
	"github.com/ghetzel/pivot/dal"
)

type Migratable interface {
	Migrate(diff []dal.SchemaDelta) error
}
