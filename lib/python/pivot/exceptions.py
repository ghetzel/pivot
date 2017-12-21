from __future__ import absolute_import


class HttpError(Exception):
    description = None

    def __init__(self, response, body):
        if isinstance(body, dict) and body.get('error'):
            super(Exception, self).__init__('HTTP {}: {}'.format(
                response.status_code,
                body['error']
            ))

        elif self.description:
            super(Exception, self).__init__('HTTP {}: {}'.format(
                response.status_code,
                self.description
            ))

        else:
            super(Exception, self).__init__('HTTP {}: {}'.format(
                response.status_code,
                body
            ))


class AuthenticationFailed(HttpError):
    description = 'Authentication Failed'


class NotFound(HttpError):
    description = 'Not Found'


class ServiceUnavailable(HttpError):
    description = 'Service Unavailable'


class CollectionNotFound(Exception):
    pass


class RecordNotFound(Exception):
    pass
