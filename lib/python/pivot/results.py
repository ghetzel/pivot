from __future__ import absolute_import
from .utils import dotdict, mutate_dict, compact


class Record(dotdict):
    def __init__(self, record):
        _record = record.get('fields', {})
        _record['id'] = record['id']

        super(dotdict, self).__init__(
            compact(
                mutate_dict(
                    _record,
                    keyFn=self.keyfn,
                    valueFn=self.valuefn
                )
            )
        )

    def keyfn(self, key, **kwargs):
        if isinstance(key, unicode):
            return str(key.encode('utf-8'))
        return key

    def valuefn(self, value):
        if isinstance(value, dict) and not isinstance(value, dotdict):
            return dotdict(value)
        return value


class RecordSet(object):
    max_repr_preview = 10

    def __init__(self, response, client=None):
        if not isinstance(response, dict):
            raise ValueError('RecordSet must be populated with a dict')

        self._client = client
        self.response = response
        self._result_count = response['result_count']
        self._results_iter = iter(self.records)

    @property
    def result_count(self):
        return self._result_count

    @property
    def records(self):
        return self.response.get('records') or []

    def __len__(self):
        return len(self.records)

    def __iter__(self):
        self._results_iter = iter(self.records)
        return self

    def __next__(self):
        if '__next__' in dir(self._results_iter):
            return Record(self._results_iter.__next__())
        else:
            return Record(self._results_iter.next())

    def next(self):
        return self.__next__()

    def __repr__(self):
        out = '<RecordSet ['
        records = self.records

        for i in range(self.max_repr_preview):
            if i < len(records):
                out += '\n  {}'.format(records[i])

        if len(records) > self.max_repr_preview:
            out += '\n  truncated [{} more, {} total]...'.format(
                len(records) - self.max_repr_preview,
                self.result_count
            )

        if out.endswith('['):
            return out + ']>'
        else:
            return out + '\n]>'

    def __str__(self):
        return self.__repr__()
