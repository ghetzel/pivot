package dal

import (
	"fmt"
)

const (
	ERR_COLLECTION_NOT_FOUND = `Collection not found`
)

var CollectionNotFound = fmt.Errorf(ERR_COLLECTION_NOT_FOUND)

func IsCollectionNotFoundErr(err error) bool {
	if err == nil {
		return false
	}

	return (err.Error() == ERR_COLLECTION_NOT_FOUND)
}
