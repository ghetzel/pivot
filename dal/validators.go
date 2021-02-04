package dal

import (
	"fmt"
	"net/url"
	"regexp"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
)

// Retrieve a validator by name.  Used by the ValidatorConfig configuration on Field.
func ValidatorFromMap(in map[string]interface{}) (FieldValidatorFunc, error) {
	validators := make([]FieldValidatorFunc, 0)

	for name, defn := range in {
		if validator, err := GetValidator(name, defn); err == nil {
			validators = append(validators, validator)
		} else {
			return nil, fmt.Errorf("Invalid validator configuration %v: %v", name, err)
		}
	}

	return ValidateAll(validators...), nil
}

// Retrieve a validator by name.  Used by the ValidatorConfig configuration on Field.
func GetValidator(name string, args interface{}) (FieldValidatorFunc, error) {
	switch name {
	case `one-of`:
		if typeutil.IsArray(args) {
			var values []interface{} = sliceutil.Sliceify(args)

			for i, v := range values {
				if vAtKey := maputil.M(v).Get(`value`); !vAtKey.IsNil() {
					values[i] = vAtKey.Value
				}
			}

			return ValidateIsOneOf(values...), nil
		} else if typeutil.IsMap(args) {
			return ValidateIsOneOf(maputil.MapValues(args)...), nil
		} else {
			return nil, fmt.Errorf("Must specify an array of values for validator 'one-of'")
		}

	case `not-zero`:
		return ValidateNonZero, nil

	case `not-empty`:
		return ValidateNotEmpty, nil

	case `positive-integer`:
		return ValidatePositiveInteger, nil

	case `positive-or-zero-integer`:
		return ValidatePositiveOrZeroInteger, nil

	case `url`:
		return ValidateIsURL, nil

	case `match`, `match-all`:
		if typeutil.IsArray(args) {
			return ValidateMatchAll(sliceutil.Stringify(args)...), nil
		} else {
			return nil, fmt.Errorf("Must specify an array of values for validator 'match'")
		}

	case `match-any`:
		if typeutil.IsArray(args) {
			return ValidateMatchAny(sliceutil.Stringify(args)...), nil
		} else {
			return nil, fmt.Errorf("Must specify an array of values for validator 'match-any'")
		}

	default:
		return nil, fmt.Errorf("Unknown validator %q", name)
	}
}

// Validate that the given value matches all of the given regular expressions.
func ValidateMatchAll(patterns ...string) FieldValidatorFunc {
	prx := make([]*regexp.Regexp, len(patterns))

	for i, rxs := range patterns {
		prx[i] = regexp.MustCompile(rxs)
	}

	return func(value interface{}) error {
		for _, rx := range prx {
			if !rx.MatchString(typeutil.String(value)) {
				return fmt.Errorf("Value does not match pattern %q", rx.String())
			}
		}

		return nil
	}
}

// Validate that the given value matches at least one of the given regular expressions.
func ValidateMatchAny(patterns ...string) FieldValidatorFunc {
	prx := make([]*regexp.Regexp, len(patterns))

	for i, rxs := range patterns {
		prx[i] = regexp.MustCompile(rxs)
	}

	return func(value interface{}) error {
		for _, rx := range prx {
			if rx.MatchString(typeutil.String(value)) {
				return nil
			}
		}

		return fmt.Errorf("Value does not match any valid pattern")
	}
}

// Validate that all of the given validator functions pass.
func ValidateAll(validators ...FieldValidatorFunc) FieldValidatorFunc {
	return func(value interface{}) error {
		for _, validator := range validators {
			if err := validator(value); err != nil {
				return err
			}
		}

		return nil
	}
}

// Validate that the given value is among the given choices.
func ValidateIsOneOf(choices ...interface{}) FieldValidatorFunc {
	return func(value interface{}) error {
		for _, choice := range choices {
			if ok, err := stringutil.RelaxedEqual(choice, value); err == nil && ok {
				return nil
			}
		}

		return fmt.Errorf("value must be one of: %+v", choices)
	}
}

// Validate that the given value is not a zero value (false, 0, 0.0, "", null).
func ValidateNonZero(value interface{}) error {
	if typeutil.IsZero(value) {
		return fmt.Errorf("expected non-zero value, got: %v", value)
	}

	return nil
}

// Validate that the given value is not a zero value, and if it's a string, that the string
// does not contain only whitespace.
func ValidateNotEmpty(value interface{}) error {
	if typeutil.IsEmpty(value) {
		return fmt.Errorf("expected non-empty value, got: %v", value)
	}

	return nil
}

// Validate that the given value is an integer > 0.
func ValidatePositiveInteger(value interface{}) error {
	if v, err := stringutil.ConvertToInteger(value); err == nil {
		if v <= 0 {
			return fmt.Errorf("expected value > 0, got: %v", v)
		}
	} else {
		return err
	}

	return nil
}

// Validate that the given value is an integer >= 0.
func ValidatePositiveOrZeroInteger(value interface{}) error {
	if v, err := stringutil.ConvertToInteger(value); err == nil {
		if v < 0 {
			return fmt.Errorf("expected value >= 0, got: %v", v)
		}
	} else {
		return err
	}

	return nil
}

// Validate that the value is a URL with a non-empty scheme and host component.
func ValidateIsURL(value interface{}) error {
	if u, err := url.Parse(typeutil.String(value)); err == nil {
		if u.Scheme == `` || u.Host == `` || u.Path == `` {
			return fmt.Errorf("Invalid URL")
		}
	} else {
		return err
	}

	return nil
}
