package dal

import (
	"fmt"

	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
)

func ValidatorFromMap(in map[string]interface{}) (FieldValidatorFunc, error) {
	validators := make([]FieldValidatorFunc, 0)

	for name, defn := range in {
		if validator, err := GetValidator(name, defn); err == nil {
			validators = append(validators, validator)
		} else {
			return nil, fmt.Errorf("Invalid validator configuration %v: %v", name, err)
		}
	}

	return ValidateAll(validators), nil
}

func GetValidator(name string, args interface{}) (FieldValidatorFunc, error) {
	switch name {
	case `one-of`:
		if typeutil.IsArray(args) {
			return ValidateIsOneOf(sliceutil.Sliceify(args)...), nil
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

	default:
		return nil, fmt.Errorf("Unknown validator %q", name)
	}
}

func ValidateAll(validators []FieldValidatorFunc) FieldValidatorFunc {
	return func(value interface{}) error {
		for _, validator := range validators {
			if err := validator(value); err != nil {
				return err
			}
		}

		return nil
	}
}

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

func ValidateNonZero(value interface{}) error {
	if typeutil.IsZero(value) {
		return fmt.Errorf("expected non-zero value, got: %v", value)
	}

	return nil
}

func ValidateNotEmpty(value interface{}) error {
	if typeutil.IsEmpty(value) {
		return fmt.Errorf("expected non-empty value, got: %v", value)
	}

	return nil
}

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
