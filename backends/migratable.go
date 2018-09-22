package backends

import (
	"github.com/ghetzel/pivot/v3/dal"
)

type Migratable interface {
	Migrate(diff []dal.SchemaDelta) error
}
