package backends

// this file satifies the Indexer interface for SqlBackend

import (
	"math"
	"reflect"

	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
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

				var totalPages int
				var totalResults int64

				// if we are paginating, then we need to do a preliminary query to get the
				// total number of records that match this query
				if f.Paginate && !f.IdOnly() {
					prequeryGen := self.makeQueryGen(collection)
					prequeryGen.Count = true

					if err := prequeryGen.Initialize(collection.Name); err == nil {
						// render the count query
						if stmt, err := filter.Render(prequeryGen, collection.Name, f); err == nil {
							values := prequeryGen.GetValues()
							querylog.Debugf("[%T] %s %v", self, string(stmt[:]), values)

							// perform the count query
							if rows, err := self.db.Query(string(stmt[:]), values...); err == nil {
								defer rows.Close()

								if rows.Next() {
									var count int64

									if err := rows.Scan(&count); err == nil {
										totalResults = count
									} else {
										return err
									}
								}

								rows.Close()
							} else {
								return err
							}
						} else {
							return err
						}
					} else {
						return err
					}

					// totalPages = ceil(result count / page size)
					totalPages = int(math.Ceil(float64(totalResults) / float64(f.Limit)))
				}

				if stmt, err := filter.Render(queryGen, collection.Name, f); err == nil {
					values := queryGen.GetValues()
					querylog.Debugf("[%T] %s %v", self, string(stmt[:]), values)

					// perform query
					if rows, err := self.db.Query(string(stmt[:]), values...); err == nil {
						defer rows.Close()

						if columns, err := rows.Columns(); err == nil {
							processedThisQuery := 0

							for rows.Next() {
								// log.Debugf("  row: %d", processed)

								if record, err := self.scanFnValueToRecord(queryGen, collection, columns, reflect.ValueOf(rows.Scan), f.Fields); err == nil {
									processed += 1
									processedThisQuery += 1

									if totalResults == 0 {
										totalResults = int64(processed)
									}

									if f.IdOnly() {
										totalPages = 1
									}

									if err := resultFn(record, nil, IndexPage{
										Page:         page,
										TotalPages:   totalPages,
										Limit:        f.Limit,
										Offset:       offset,
										TotalResults: totalResults,
									}); err != nil {
										return err
									}
								} else {
									if err := resultFn(dal.NewRecord(nil).Set(`error`, err.Error()), err, IndexPage{}); err != nil {
										return err
									}

									// if the resultFn didn't stop us, move on to the next row
									continue
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
						if err := resultFn(nil, err, IndexPage{}); err != nil {
							return err
						}
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

func (self *SqlBackend) Query(collection string, f filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	return DefaultQueryImplementation(self, collection, f, resultFns...)
}

func (self *SqlBackend) ListValues(collectionName string, fields []string, f filter.Filter) (map[string][]interface{}, error) {
	if collection, err := self.getCollectionFromCache(collectionName); err == nil {
		for i, f := range fields {
			if f == `id` {
				fields[i] = collection.IdentityField
			}
		}

		output := make(map[string][]interface{})

		for _, field := range fields {
			f.Fields = []string{field}
			f.Options[`Distinct`] = true

			if results, err := self.Query(collectionName, f); err == nil {
				var values []interface{}

				if v, ok := output[field]; ok {
					values = v
				} else {
					values = make([]interface{}, 0)
				}

				if field == collection.IdentityField {
					for _, result := range results.Records {
						values = append(values, result.ID)
					}
				} else {
					values = sliceutil.Compact(results.Pluck(field))
				}

				output[field] = values
			} else {
				return nil, err
			}
		}

		return output, nil
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

// DeleteQuery removes records using a filter
func (self *SqlBackend) DeleteQuery(name string, f filter.Filter) error {
	if collection, err := self.getCollectionFromCache(name); err == nil {
		if tx, err := self.db.Begin(); err == nil {
			queryGen := self.makeQueryGen(collection)
			queryGen.Type = generators.SqlDeleteStatement

			// generate SQL
			if stmt, err := filter.Render(queryGen, collection.Name, f); err == nil {
				querylog.Debugf("[%T] %s %v", self, string(stmt[:]), queryGen.GetValues())

				// execute SQL
				if _, err := tx.Exec(string(stmt[:]), queryGen.GetValues()...); err == nil {
					if err := tx.Commit(); err == nil {
						return nil
					} else {
						return err
					}
				} else {
					defer tx.Rollback()
					return err
				}
			} else {
				defer tx.Rollback()
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *SqlBackend) FlushIndex() error {
	return nil
}
