package pivot

import (
	"github.com/ghetzel/pivot/"
	"github.com/ghetzel/pivot/backends"
	"github.com/stretchr/testify/require"
	"testing"
)

func makeBackend(t *require.Assertions, conn string) (backends.Backend, error) {
	if cs, err := dal.ParseConnectionString(conn); err == nil {
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

func TestCollectionManagement(t *testing.T) {
	assert := require.New(t)

	if backend, err := makeBackend(assert, `bolt:///./test.db`); err == nil {
		err := backend.CreateCollection(dal.Collection{
			Name: `test1`,
		})

		assert.Nil(err)
	} else {
		assert.Nil(err)
	}
}
