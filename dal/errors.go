package dal

import (
	"errors"
	"strings"
)

var CollectionNotFound = errors.New(`Collection not found`)
var FieldNotFound = errors.New(`Field not found`)

func IsCollectionNotFoundErr(err error) bool {
	return (err == CollectionNotFound)
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

func IsFieldNotFoundErr(err error) bool {
	return (err == FieldNotFound)
}
