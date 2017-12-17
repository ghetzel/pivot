from __future__ import absolute_import


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
        self.client = client
        self._definition = None

    def load(self):
        if self.client:
            self._definition = self.client.request(
                'get',
                '/api/collections/' + self.name
            ).json()

    def __str__(self):
        return '<Collection {} fields={}>'.format(
            self.name,
            len(self.definition.get('fields', []))
        )

    def __repr__(self):
        return self.__str__()

    @property
    def definition(self):
        if not self._definition:
            self.load()

        return self._definition

    @property
    def fields(self):
        return [Field(f) for f in self.definition.get('fields', [])]
