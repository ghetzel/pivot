package dal

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
)

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
			if ok, err := typeutil.RelaxedEqual(choice, value); err == nil && ok {
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
