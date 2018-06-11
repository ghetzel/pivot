# Pivot Python Client Library

## Overview

This is a Python module that is used as a client for interacting with the Pivot database
abstraction service.  Import this into your Python projects to interact with databases
using Pivot.


## Installation

To use this module, install it using the Python `pip` utility (directly, or via a _requirements.txt_
dependency file) like so:

```
pip install git+https://github.com/ghetzel/pivot.git#subdirectory=lib/python
```

## Examples

Here are some examples for working with data using this module:

### Query data from an existing collection

```python
import pivot

# Connect to a local Pivot instance running at http://localhost:29029
client = pivot.Client()

# get details about the "users" Collection
users = client.collection('users')

# query all users, print out their records
for user in users.all(limit=False, sort):
    print('User {} (id={})'.format(user.name, user.id))
    print('  email: {}'.format(user.email))
    print('  roles: {}'.format( ','.join(user.roles or []) )
```


### Create a new record in the "orders" collection

```python
import pivot

# Connect to a local Pivot instance running at http://localhost:29029
client = pivot.Client()
orders = client.collection('orders')

# Create a new record in the orders collection.  Depending on the collection's
# schema definition, additional fields may be added with default values that
# aren't explicitly specified here.  If any required fields are missing, this
# call will raise a `pivot.exceptions.HttpError` exception containing the error
# that Pivot returned.
#
orders.create({
    'user_id':  123,
    'item_ids': [4, 8, 12],
    'shipping_address': '123 Fake St., Anytown, MO, 64141',
})
```
