package dal

import (
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ghetzel/go-stockutil/sliceutil"
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

func FormatterFromMap(in map[string]interface{}) (FieldFormatterFunc, error) {
	formatters := make([]FieldFormatterFunc, 0)

	for name, defn := range in {
		if formatter, err := GetFormatter(name, defn); err == nil {
			formatters = append(formatters, formatter)
		} else {
			return nil, fmt.Errorf("Invalid formatter configuration %v: %v", name, err)
		}
	}

	return FormatAll(formatters...), nil
}

func GetFormatter(name string, args interface{}) (FieldFormatterFunc, error) {
	switch name {
	case `uuid`:
		return GenerateUUID, nil

	case `encoded-uuid`:
		var encoder EncoderFunc

		switch fmt.Sprintf("%v", args) {
		case `base32`:
			encoder = Base32Encoder
		case `base58`:
			encoder = Base58Encoder
		case `base64`:
			encoder = Base64Encoder
		default:
			encoder = HexEncoder
		}

		return GenerateEncodedUUID(encoder), nil

	case `trim-space`:
		return TrimSpace, nil

	case `change-case`:
		return ChangeCase(sliceutil.Stringify(args)...), nil

	case `replace`:
		return Replace(sliceutil.Sliceify(args)), nil

	// case `fields`:
	// 	if typeutil.IsMap(args) {

	// 		return DeriveFromFields()
	// 	}

	case `current-time`:
		return CurrentTime, nil

	case `current-time-if-unset`:
		return CurrentTimeIfUnset, nil

	case `now-plus-duration`:
		return NowPlusDuration(typeutil.V(args).Duration()), nil

	default:
		return nil, fmt.Errorf("Unknown formatter %q", name)
	}
}

func FormatAll(formatters ...FieldFormatterFunc) FieldFormatterFunc {
	return func(value interface{}, op FieldOperation) (interface{}, error) {
		for _, formatter := range formatters {
			if v, err := formatter(value, op); err == nil {
				value = v
			} else {
				return nil, err
			}
		}

		return value, nil
	}
}

func ChangeCase(cases ...string) FieldFormatterFunc {
	return func(value interface{}, _ FieldOperation) (interface{}, error) {
		if record, ok := value.(*Record); ok {
			value = record.ID
		}

		if vStr, err := stringutil.ToString(value); err == nil {
			for _, c := range cases {
				switch c {
				case `upper`:
					vStr = strings.ToUpper(vStr)
				case `lower`:
					vStr = strings.ToLower(vStr)
				case `camelize`:
					vStr = stringutil.Camelize(vStr)
				case `hyphenate`:
					vStr = stringutil.Hyphenate(vStr)
				case `underscore`:
					vStr = stringutil.Underscore(vStr)
				case `title`:
					vStr = strings.Title(vStr)
				default:
					return nil, fmt.Errorf("Unsupported case '%s'", c)
				}
			}

			return vStr, nil
		} else {
			return value, err
		}
	}
}

func Replace(pairs []interface{}) FieldFormatterFunc {
	return func(value interface{}, _ FieldOperation) (interface{}, error) {
		if vStr, err := stringutil.ToString(value); err == nil {
			for _, pair := range pairs {
				p := sliceutil.Stringify(pair)

				if len(p) != 2 {
					return nil, fmt.Errorf("'replace' formatter requires an argument of [[FINDPATTERN, REPLACEWITH], ..]")
				} else {
					find := p[0]
					replace := p[1]

					if record, ok := value.(*Record); ok {
						value = record.ID
					}

					if rx, err := regexp.Compile(find); err == nil {
						vStr = rx.ReplaceAllString(vStr, replace)
					} else {
						return value, fmt.Errorf("invalid find pattern: %v", err)
					}
				}
			}

			return vStr, nil
		} else {
			return value, err
		}
	}
}

func TrimSpace(value interface{}, _ FieldOperation) (interface{}, error) {
	if record, ok := value.(*Record); ok {
		value = record.ID
	}

	if vStr, err := stringutil.ToString(value); err == nil {
		return strings.TrimSpace(vStr), nil
	} else {
		return value, err
	}
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
	return time.Now(), nil
}

func CurrentTimeIfUnset(value interface{}, op FieldOperation) (interface{}, error) {
	if typeutil.IsZero(value) {
		return time.Now(), nil
	}

	return value, nil
}

func NowPlusDuration(duration time.Duration) FieldFormatterFunc {
	return func(value interface{}, op FieldOperation) (interface{}, error) {
		if duration != 0 {
			return time.Now().Add(duration), nil
		} else {
			return value, nil
		}
	}
}
