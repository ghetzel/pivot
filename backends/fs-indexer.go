package backends

import (
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

func (self *FilesystemBackend) IndexConnectionString() *dal.ConnectionString {
	return &dal.ConnectionString{}
}

func (self *FilesystemBackend) IndexInitialize(_ Backend) error {
	return nil
}

func (self *FilesystemBackend) GetBackend() Backend {
	return self
}

func (self *FilesystemBackend) IndexExists(collection *dal.Collection, id interface{}) bool {
	return self.Exists(collection.GetIndexName(), id)
}

func (self *FilesystemBackend) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	defer stats.NewTiming().Send(`pivot.indexers.filesystem.retrieve_time`)
	return self.Retrieve(collection.GetIndexName(), id)
}

func (self *FilesystemBackend) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return nil
}

func (self *FilesystemBackend) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return nil
}

func (self *FilesystemBackend) QueryFunc(collection *dal.Collection, filter *filter.Filter, resultFn IndexResultFunc) error {
	defer stats.NewTiming().Send(`pivot.indexers.filesystem.query_time`)
	querylog.Debugf("[%T] Query using filter %q", self, filter.String())

	if filter.IdOnly() {
		if id, ok := filter.GetFirstValue(); ok {
			if record, err := self.Retrieve(collection.GetIndexName(), id); err == nil {
				querylog.Debugf("[%T] Record %v matches filter %q", self, id, filter.String())

				if err := resultFn(record, err, IndexPage{
					Page:         1,
					TotalPages:   1,
					Limit:        filter.Limit,
					Offset:       0,
					TotalResults: 1,
				}); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	} else {
		if ids, err := self.listObjectIdsInCollection(collection); err == nil {
			page := 1
			processed := 0
			offset := filter.Offset

			for _, id := range ids {
				// retrieve the record by id
				if record, err := self.Retrieve(collection.Name, id); err == nil {
					record.ID = stringutil.Autotype(record.ID)

					// if matching all records OR the found record matches the filter
					if filter.MatchesRecord(record) {
						if processed >= offset {
							querylog.Debugf("[%T] Record %v matches filter %q", self, record.ID, filter.String())

							if err := resultFn(record, err, IndexPage{
								Page:         page,
								TotalPages:   1,
								Limit:        filter.Limit,
								Offset:       offset,
								TotalResults: -1,
							}); err != nil {
								return err
							}
						}
					}
				} else {
					if err := resultFn(dal.NewRecord(nil), err, IndexPage{
						Page:         page,
						TotalPages:   1,
						Limit:        filter.Limit,
						Offset:       offset,
						TotalResults: -1,
					}); err != nil {
						return err
					}
				}

				processed += 1
				page = int(float64(processed) / float64(filter.Limit))

				if filter.Limit > 0 && processed >= (offset+filter.Limit) {
					querylog.Debugf("[%T] %d at or beyond limit %d, returning results", self, processed, filter.Limit)
					break
				}
			}
		} else {
			return err
		}
	}

	return nil
}

func (self *FilesystemBackend) Query(collection *dal.Collection, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	return DefaultQueryImplementation(self, collection, f, resultFns...)
}

func (self *FilesystemBackend) ListValues(collection *dal.Collection, fields []string, f *filter.Filter) (map[string][]interface{}, error) {
	values := make(map[string][]interface{})

	if err := self.QueryFunc(collection, f, func(record *dal.Record, err error, page IndexPage) error {
		if err == nil {
			for _, field := range fields {
				var v []interface{}

				switch field {
				case collection.IdentityField:
					field = collection.IdentityField

					if current, ok := values[field]; ok {
						v = current
					} else {
						v = make([]interface{}, 0)
					}

					if record.ID != nil {
						v = sliceutil.Unique(append(v, record.ID))
					}
				default:
					if current, ok := values[field]; ok {
						v = current
					} else {
						v = make([]interface{}, 0)
					}

					if newV := record.Get(field); newV != nil {
						v = sliceutil.Unique(append(v, newV))
					}
				}

				values[field] = v
			}
		}

		return nil
	}); err == nil {
		return values, nil
	} else {
		return values, err
	}
}

func (self *FilesystemBackend) DeleteQuery(collection *dal.Collection, f *filter.Filter) error {
	idsToRemove := make([]interface{}, 0)

	if err := self.QueryFunc(collection, f, func(record *dal.Record, err error, page IndexPage) error {
		if err == nil {
			idsToRemove = append(idsToRemove, record.ID)
		}

		return nil
	}); err == nil {
		return self.Delete(collection.Name, idsToRemove...)
	} else {
		return err
	}
}

func (self *FilesystemBackend) FlushIndex() error {
	return nil
}
