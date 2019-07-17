# Filter Syntax

Pivot uses a simplified filter syntax that is URL-friendly and fairly straightforward to use.  It consists of a sequence of `field/value` pairs that are themselves separated by a forward slash.  Generally, the best way to learn this syntax is through examples:

## Examples

```
# Where field "id" is exactly equal to 123
id/123

# Where "id" is exactly 3, 14, OR 27
id/3|14|27

# Where "id" is exactly 3, 14, OR 27 AND "enabled" is true
id/3|14|27/enabled/true

# Where "name" is "Bob" AND "age" is 42.
name/Bob/age/42

# Where "product" contains the string "usb" and "price" is less than 5.00
product/contains:usb/price/lt:5.00

# Where "product" contains the string "usb" and "price" is between 10.00 (inclusive) and 20.01 (exclusive)
product/contains:usb/price/range:10|20.01
```

## General Form

The general form of this filter syntax is:

`[[type:]field/[operator:]value[|orvalue ..] ..]`

## Operators

Supported operators are as follows:

| Operator   | Description |
| ---------- | ----------- |
| `is`       | Values must exactly match (this is the default if no operator is provided) |
| `not`      | Values may be anything _except_ an exact match |
| `contains` | String value must contain the given substring (case sensitive) |
| `like`     | String value must contain the given substring (case insensitive) |
| `unlike`   | String value may not contain the given substring (case insensitive) |
| `prefix`   | String value must start with the given string |
| `suffix`   | String value must end with the given string |
| `gt`       | Numeric or date value must be strictly greater than |
| `gte`      | Numeric or date value must be strictly greater than or equal to |
| `lt`       | Numeric or date value must be strictly less than |
| `lte`      | Numeric or date value must be strictly less than or equal to |
| `range`    | Numeric or date value must be between two values (separated by `|`; first value is inclusive, second value exclusive |


