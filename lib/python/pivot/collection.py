from __future__ import absolute_import
from .results import ResultSet, Record
from . import exceptions


class Field(object):
    def __init__(self, config):
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
    def __init__(self, name, client=None):
        self.name = name
        self._client = client
        self._definition = None

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
        return [Field(f) for f in self.definition.get('fields', [])]

    def query(self, filterstring):
        """
        Return a ResultSet of Records matching the given query against this collection.
        """
        results = self.client.request(
            'get',
            '/api/collections/{}/where/{}'.format(self.name, filterstring)
        ).json()

        return ResultSet(results, client=self.client)

    def get(self, rid):
        """
        Retrieve a specific record by its ID from this collection.
        """
        try:
            return Record(self.client.request(
                'get',
                '/api/collections/{}/records/{}'.format(self.name, rid)
            ).json())
        except exceptions.NotFound:
            raise exceptions.RecordNotFound()
        except:
            raise

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
