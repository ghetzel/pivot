package dal

import (
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/jbenet/go-base58"
)

type EncoderFunc func([]byte) (string, error) //{}

var Base32Encoder = func(src []byte) (string, error) {
	return strings.TrimSuffix(base32.StdEncoding.EncodeToString(src), `=`), nil
}

var Base58Encoder = func(src []byte) (string, error) {
	return base58.Encode(src), nil
}

var Base64Encoder = func(src []byte) (string, error) {
	return strings.TrimSuffix(base64.StdEncoding.EncodeToString(src), `=`), nil
}

var HexEncoder = func(src []byte) (string, error) {
	return hex.EncodeToString(src), nil
}

func GenerateUUID(value interface{}, _ FieldOperation) (interface{}, error) {
	if record, ok := value.(*Record); ok {
		value = record.ID
	}

	if typeutil.IsZero(value) {
		value = stringutil.UUID().String()
	}

	return value, nil
}

func GenerateEncodedUUID(encoder EncoderFunc) FieldFormatterFunc {
	return func(value interface{}, _ FieldOperation) (interface{}, error) {
		if record, ok := value.(*Record); ok {
			value = record.ID
		}

		if typeutil.IsZero(value) {
			if v, err := encoder(stringutil.UUID().Bytes()); err == nil {
				if typeutil.IsZero(v) {
					return value, fmt.Errorf("UUID encoder produced a zero-length result")
				}

				value = v
			} else {
				return value, err
			}
		}

		return value, nil
	}
}

func DeriveFromFields(format string, fields ...string) FieldFormatterFunc {
	return func(input interface{}, _ FieldOperation) (interface{}, error) {
		if record, ok := input.(*Record); ok {
			values := make([]interface{}, len(fields))

			for i, field := range fields {
				values[i] = record.Get(field)
			}

			return fmt.Sprintf(format, values...), nil
		} else {
			return nil, fmt.Errorf("DeriveFromFields formatter requires a *dal.Record argument, got %T", input)
		}
	}
}

func CurrentTime(value interface{}, op FieldOperation) (interface{}, error) {
	if op == PersistOperation {
		return time.Now(), nil
	} else {
		return value, nil
	}
}

func CurrentTimeIfUnset(value interface{}, op FieldOperation) (interface{}, error) {
	if op == PersistOperation && typeutil.IsZero(value) {
		return time.Now(), nil
	}

	return value, nil
}
