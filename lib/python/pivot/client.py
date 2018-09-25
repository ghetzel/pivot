from __future__ import unicode_literals
from __future__ import absolute_import
import requests
import requests.exceptions
from . import exceptions
from .collection import Collection, Field
from .utils import compact


DEFAULT_URL = 'http://localhost:29029'


class Client(object):
    def __init__(self, url=DEFAULT_URL):
        # disables the warnings requests emits, which ARE for our own good, but if we make the
        # decision to do something stupid, we'll own that and don't need to pollute the logs.
        requests.packages.urllib3.disable_warnings()
        self.url = url
        self.session = requests.Session()

    def request(self, method, path, data=None, params={}, headers={}, **kwargs):
        headers['Content-Type'] = 'application/json'

        response = getattr(self.session, method.lower())(
            self.make_url(path),
            json=data,
            params=params,
            headers=headers,
            **kwargs
        )

        if response.status_code < 400:
            return response
        else:
            body = response.json()

            if response.status_code == 403:
                raise exceptions.AuthenticationFailed(response, body)
            elif response.status_code == 404:
                raise exceptions.NotFound(response, body)
            elif response.status_code >= 500:
                raise exceptions.ServiceUnavailable(response, body)
            else:
                raise exceptions.HttpError(response, body)

    def make_url(self, path):
        return '{}/{}'.format(self.url, path.lstrip('/'))

    @property
    def collections(self):
        return [
            Collection(c, client=self) for c in self.request(
                'get',
                '/api/schema'
            ).json()
        ]

    def collection(self, name):
        c = Collection(name, client=self)
        c.load()
        return c

    def create_collection(
        self,
        name,
        identity_field=None,
        identity_field_type=None,
        fields=[]
    ):
        for i, field in enumerate(fields):
            if isinstance(field, Field):
                fields[i] = field.as_dict()
            elif isinstance(field, dict):
                continue
            else:
                raise TypeError("Field specification must be a dict or a Field")

        body = compact({
            'name':                name,
            'identity_field':      identity_field,
            'identity_field_type': identity_field_type,
            'fields':              fields,
        })

        self.request('post', '/api/schema', body).json()

        return Collection(name, client=self)

    def delete_collection(self, name):
        self.request('delete', '/api/schema/' + name)
        return True
