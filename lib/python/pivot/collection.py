from __future__ import unicode_literals
from __future__ import absolute_import
from .results import RecordSet, Record
from . import exceptions
from .utils import compact
import json


class Field(object):
    def __init__(self, **config):
        self.name = None
        self.type = None
        self.identity = False
        self.key = False
        self.required = False
        self.default = None
        self.native_type = None

        if isinstance(config, dict):
            for k, v in config.items():
                setattr(self, k, v)

        if self.name is None:
            raise ValueError("Field name is required")

        if self.type is None:
            raise ValueError("Field type is required")

    def as_dict(self):
        defn = {
            'name': self.name,
            'type': self.type,
        }

        if self.identity:
            defn['identity'] = True
        elif self.key:
            defn['key'] = True

        if self.required:
            defn['required'] = True

        if self.default is not None:
            defn['default'] = self.default

        return defn

    def __str__(self):
        s = '<Field {} ({})'.format(
            self.name,
            self.type
        )

        if self.identity:
            s += ' identity'
        elif self.key:
            s += ' key'

        if self.required:
            s += ' required'

        s += '>'

        return s

    def __repr__(self):
        return self.__str__()


class Collection(object):
    def __init__(self, name, client=None, definition=None):
        self.name = name
        self._client = client
        self._definition = definition

        try:
            name = self._definition['name']

            if name:
                self.name = name
        except:
            pass

    @property
    def client(self):
        """
        Return the Pivot client instance, or raise an exception if there is none.
        """
        if self._client:
            return self._client

        raise Exception("No client specified for Collection {}".format(self.name))

    @property
    def definition(self):
        """
        Return the schema definition for this collection.
        """
        if not self._definition:
            self.load()

        return self._definition

    @property
    def fields(self):
        """
        Return a list of Field objects describing this collection's schema.
        """
        return [Field(**f) for f in self.definition.get('fields', [])]

    def all(self, **kwargs):
        return self.query('all', **kwargs)

    def query(self, filterstring, limit=None, offset=None, sort=None, fields=None, conjunction=None, noexpand=None):
        """
        Return a RecordSet of Records matching the given query against this collection.
        """
        if fields is None:
            fields = []
        if not isinstance(fields, list):
            fields = [fields]

        if sort is None:
            sort = []
        if not isinstance(sort, list):
            sort = [sort]

        if limit is False:
            limit = 2147483647

        results = self.client.request(
            'get',
            '/api/collections/{}/where/{}'.format(self.name, filterstring),
            params=compact({
                'limit':       limit,
                'offset':      offset,
                'sort':        ','.join(sort),
                'fields':      ','.join(fields),
                'conjunction': conjunction,
                'noexpand':    noexpand,
            })
        ).json()

        return RecordSet(results, client=self.client)

    def get(self, rid, fields=None, noexpand=None):
        """
        Retrieve a specific record by its ID from this collection.
        """
        if fields is None:
            fields = []
        if not isinstance(fields, list):
            fields = [fields]

        try:
            return Record(self.client.request(
                'get',
                '/api/collections/{}/records/{}'.format(self.name, rid),
                params=compact({
                    'fields': ','.join(fields),
                    'noexpand':    noexpand,
                })
            ).json())
        except exceptions.NotFound:
            raise exceptions.RecordNotFound()
        except:
            raise

    def delete(self, *ids):
        ids = '/'.join([str(rid) for rid in ids])

        if len(ids):
            self.client.request(
                'delete',
                '/api/collections/{}/records/{}'.format(self.name, ids)
            )

        return

    def create(self, *records, **kwargs):
        update = kwargs.pop('update', False)
        options = {}
        diffuse = kwargs.pop('diffuse', '')

        records = [{
            'id':     record.pop('id', None),
            'fields': record,
        } for record in [dict(r) for r in records]]

        if len(diffuse):
            options['diffuse'] = diffuse

        response = self.client.request(
            ('put' if update else 'post'),
            '/api/collections/{}/records?{}'.format(
                self.name,
                '&'.join(['{}={}'.format(k, v) for k, v in options.items()])
            ),
            {
                'records': records,
            }
        )

        if len(response.content):
            return RecordSet(response.json())
        else:
            return RecordSet({})

    def update(self, *records):
        return self.create(*records, update=True)

    def update_or_create(self, *records):
        try:
            return self.update(*records)
        except Exception as e:
            if 'Cannot update record without an ID' in '{}'.format(e):
                return self.create(*records)
            else:
                raise

    def aggregate(self, fields, fns=None, filterstring=None, noexpand=None):
        """
        Return a dict of field-aggregates for the given fields, optionally specifying
        additional aggregation functions and a pre-aggregation filter.
        """
        if fields is None:
            fields = ['_id']
        if not isinstance(fields, list):
            fields = [fields]

        return self.client.request(
            'get',
            '/api/collections/{}/aggregate/{}'.format(self.name, ','.join(fields)),
            params=compact({
                'fn':       ','.join(fns),
                'q':        filterstring,
                'noexpand': noexpand,
            })
        ).json()

    def count(self, filterstring=None, noexpand=None):
        """
        Return a count of all records, or just those matching the given filter.
        """
        return self.aggregate(
            None,
            fns=['count'],
            filterstring=filterstring,
            noexpand=noexpand
        ).get(
            '_id', {}
        ).get('count', 0)

    def sum(self, field, filterstring=None, noexpand=None):
        """
        Return the sum of all values of the given field, optionally filtered by
        the given filterstring.
        """
        return float(self.aggregate(
            field,
            fns=['sum'],
            filterstring=filterstring,
            noexpand=noexpand
        ).get(
            field, {}
        ).get('sum', 0))

    def average(self, field, filterstring=None, noexpand=None):
        """
        Return the arithmetic mean of all values of the given field, optionally
        filtered by then given filterstring.
        """
        return float(self.aggregate(
            field,
            fns=['avg'],
            filterstring=filterstring,
            noexpand=noexpand
        ).get(
            field, {}
        ).get('avg', 0))

    def minimum(self, field, filterstring=None, noexpand=None):
        """
        Return the minimum of all values of the given field, optionally
        filtered by then given filterstring.
        """
        return float(self.aggregate(
            field,
            fns=['min'],
            filterstring=filterstring,
            noexpand=noexpand
        ).get(
            field, {}
        ).get('min', 0))

    def maximum(self, field, filterstring=None, noexpand=None):
        """
        Return the maximum of all values of the given field, optionally
        filtered by then given filterstring.
        """
        return float(self.aggregate(
            field,
            fns=['max'],
            filterstring=filterstring,
            noexpand=noexpand
        ).get(
            field, {}
        ).get('max', 0))

    def __len__(self):
        """
        Implements len(collection)
        """
        return self.count()

    def load(self):
        """
        Load this collection's definition from Pivot.
        """
        try:
            self._definition = self.client.request(
                'get',
                '/api/collections/{}'.format(self.name)
            ).json()
        except exceptions.NotFound:
            raise exceptions.CollectionNotFound()
        except:
            raise

    def __str__(self):
        return '<Collection {} fields={}>'.format(
            self.name,
            len(self.definition.get('fields', []))
        )

    def __repr__(self):
        return self.__str__()
