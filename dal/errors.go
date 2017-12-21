package dal

import (
	"fmt"
	"strings"
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

func IsNotExistError(err error) bool {
	if err == nil {
		return false
	}

	return strings.HasSuffix(err.Error(), ` does not exist`)
}

func IsExistError(err error) bool {
	if err == nil {
		return false
	}

	return strings.HasSuffix(err.Error(), ` already exists`)
}
