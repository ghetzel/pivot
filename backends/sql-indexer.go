package backends

// this file satifies the Indexer interface for SqlBackend

import (
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"reflect"
)

func (self *SqlBackend) QueryFunc(collectionName string, f filter.Filter, resultFn IndexResultFunc) error {
	defer stats.NewTiming().Send(`pivot.backends.sql.query_time`)

	if collection, err := self.getCollectionFromCache(collectionName); err == nil {
		f.IdentityField = collection.IdentityField
		page := 1
		processed := 0
		offset := f.Offset

		if f.Limit == 0 && f.Offset > 0 {
			f.Limit = IndexerPageSize
		}

		for {
			queryGen := self.makeQueryGen(collection)

			if err := f.ApplyOptions(&queryGen); err != nil {
				return nil
			}

			if err := queryGen.Initialize(collection.Name); err == nil {
				f.Offset = offset

				if sqlString, err := filter.Render(queryGen, collection.Name, f); err == nil {
					// log.Debugf("%s %+v; processed=%d", string(sqlString[:]), queryGen.GetValues(), processed)

					// perform query
					if rows, err := self.db.Query(string(sqlString[:]), queryGen.GetValues()...); err == nil {
						defer rows.Close()

						if columns, err := rows.Columns(); err == nil {
							processedThisQuery := 0

							for rows.Next() {
								// log.Debugf("  row: %d", processed)

								if record, err := self.scanFnValueToRecord(collection, columns, reflect.ValueOf(rows.Scan)); err == nil {
									processed += 1
									processedThisQuery += 1

									if err := resultFn(record, IndexPage{
										Page:         page,
										TotalPages:   0,
										Limit:        f.Limit,
										Offset:       offset,
										TotalResults: int64(processed),
									}); err != nil {
										return err
									}
								} else {
									return err
								}
							}

							// if the number of records we just processed was less than the limit we set,
							// break early
							if processedThisQuery <= f.Limit || f.Limit == 0 {
								// log.Debugf("returning: ptd=%d, ptotal=%d", processedThisQuery, processed)
								return nil
							}

							rows.Close()

							// increment offset by the page size we just processed
							page += 1
							offset += processedThisQuery
						} else {
							return err
						}
					} else {
						return err
					}

				} else {
					return err
				}
			} else {
				return err
			}
		}
	} else {
		return dal.CollectionNotFound
	}
}

func (self *SqlBackend) Query(collection string, f filter.Filter) (*dal.RecordSet, error) {
	recordset := dal.NewRecordSet()

	// TODO: figure out a smart way to get row counts so that we can offer bounded/paginated resultsets
	recordset.Unbounded = true

	if err := self.QueryFunc(collection, f, func(record *dal.Record, page IndexPage) error {
		PopulateRecordSetPageDetails(recordset, f, page)

		if f.IdOnly() {
			recordset.Records = append(recordset.Records, dal.NewRecord(record.ID))
		} else {
			recordset.Records = append(recordset.Records, record)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return recordset, nil
}
func (self *SqlBackend) ListValues(collectionName string, fields []string, f filter.Filter) (*dal.RecordSet, error) {
	if collection, err := self.getCollectionFromCache(collectionName); err == nil {
		for i, f := range fields {
			if f == `id` {
				fields[i] = collection.IdentityField
			}
		}

		recordset := dal.NewRecordSet()
		groupedByField := make(map[string]*dal.Record)

		for _, field := range fields {
			f.Fields = []string{field}
			f.Options[`Distinct`] = true

			if results, err := self.Query(collectionName, f); err == nil {
				var record *dal.Record

				if r, ok := groupedByField[field]; ok {
					record = r
				} else {
					record = dal.NewRecord(field)
					groupedByField[field] = record
				}

				var values []interface{}

				if field == collection.IdentityField {
					values = make([]interface{}, 0)

					for _, result := range results.Records {
						values = append(values, result.ID)
					}
				} else {
					values = sliceutil.Compact(results.Pluck(field))
				}

				record.Set(`values`, values)

				recordset.Push(record)
			} else {
				return nil, err
			}
		}

		return recordset, nil
	} else {
		return nil, err
	}
}

func (self *SqlBackend) IndexConnectionString() *dal.ConnectionString {
	return self.GetConnectionString()
}

func (self *SqlBackend) IndexInitialize(parent Backend) error {
	return nil
}

func (self *SqlBackend) IndexExists(collection string, id interface{}) bool {
	return self.Exists(collection, id)
}

func (self *SqlBackend) IndexRetrieve(collection string, id interface{}) (*dal.Record, error) {
	return self.Retrieve(collection, id)
}

// Index is a no-op, this should be handled by SqlBackend's Insert() function
func (self *SqlBackend) Index(collection string, records *dal.RecordSet) error {
	return nil
}

// IndexRemove is a no-op, this should be handled by SqlBackend's Delete() function
func (self *SqlBackend) IndexRemove(collection string, ids []interface{}) error {
	return nil
}
