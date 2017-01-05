package backends

// this file satifies the Indexer interface for SqlBackend

import (
	"fmt"
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
			queryGen := self.makeQueryGen()

			if err := queryGen.Initialize(collection.Name); err == nil {
				f.Offset = offset

				if sqlString, err := filter.Render(queryGen, collection.Name, f); err == nil {
					log.Debugf("%s %+v; ptq=%d", string(sqlString[:]), queryGen.GetValues(), processed)

					// perform query
					if rows, err := self.db.Query(string(sqlString[:]), queryGen.GetValues()...); err == nil {
						defer rows.Close()

						if columns, err := rows.Columns(); err == nil {
							processedThisQuery := 0

							for rows.Next() {
								log.Debugf("  row: %d", processed)

								if record, err := self.scanFnValueToRecord(collection, columns, reflect.ValueOf(rows.Scan)); err == nil {
									processed += 1
									processedThisQuery += 1

									if err := resultFn(record, IndexPage{
										Page:         page,
										TotalPages:   0,
										Limit:        f.Limit,
										Offset:       offset,
										TotalResults: uint64(processed),
									}); err != nil {
										return err
									}
								} else {
									return err
								}
							}

							// if the number of records we just processed was less than the limit we set,
							// break early
							if processedThisQuery <= f.Limit {
								log.Debugf("returning: ptd=%d, ptotal=%d", processedThisQuery, processed)
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
func (self *SqlBackend) ListValues(collection string, fields []string, f filter.Filter) (*dal.RecordSet, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (self *SqlBackend) IndexInitialize(parent Backend) error {
	return nil
}

func (self *SqlBackend) IndexExists(collection string, id string) bool {
	return self.Exists(collection, id)
}

func (self *SqlBackend) IndexRetrieve(collection string, id string) (*dal.Record, error) {
	return self.Retrieve(collection, id)
}

// Index is a no-op, this should be handled by SqlBackend's Insert() function
func (self *SqlBackend) Index(collection string, records *dal.RecordSet) error {
	return nil
}

// IndexRemove is a no-op, this should be handled by SqlBackend's Delete() function
func (self *SqlBackend) IndexRemove(collection string, ids []string) error {
	return nil
}
