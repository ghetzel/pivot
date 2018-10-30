package backends

import (
	"fmt"
	"strings"

	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type DeferredRecord struct {
	Original       interface{}
	Backend        Backend
	CollectionName string
	ID             interface{}
	Keys           []string
	AllowMissing   bool
}

func (self *DeferredRecord) GroupKey(id ...interface{}) string {
	base := fmt.Sprintf("%v:%v", self.Backend.String(), self.CollectionName)

	if len(id) > 0 {
		base += fmt.Sprintf(":%v", id[0])
	}

	if len(self.Keys) > 0 {
		return base + `@` + strings.Join(self.Keys, `,`)
	} else {
		return base
	}
}

func (self *DeferredRecord) String() string {
	return self.GroupKey(self.ID)
}

type recordFieldValue struct {
	Record   *dal.Record
	Key      []string
	Value    interface{}
	Deferred *DeferredRecord
}

func (self *DeferredRecord) Resolve() (map[string]interface{}, error) {
	if name := self.CollectionName; name != `` {
		if record, err := self.Backend.Retrieve(name, self.ID, self.Keys...); err == nil {
			return record.Fields, nil
		} else if self.AllowMissing {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("collection not specified")
	}
}

func PopulateRelationships(backend Backend, parent *dal.Collection, record *dal.Record, prepId func(interface{}) interface{}, requestedFields ...string) error { // for each relationship
	skipKeys := make([]string, 0)

	if embed, ok := backend.(*EmbeddedRecordBackend); ok {
		skipKeys = embed.SkipKeys
	}

	for _, relationship := range parent.EmbeddedCollections {
		keys := sliceutil.CompactString(sliceutil.Stringify(sliceutil.Sliceify(relationship.Keys)))

		// if we're supposed to skip certain keys, and this is one of them
		if len(skipKeys) > 0 && sliceutil.ContainsAnyString(skipKeys, keys...) {
			log.Debugf("explicitly skipping %+v", keys)
			continue
		}

		var related *dal.Collection

		if relationship.Collection != nil {
			related = relationship.Collection
		} else if c, err := backend.GetCollection(relationship.CollectionName); c != nil {
			related = c
		} else {
			return fmt.Errorf("error in relationship %v: %v", keys, err)
		}

		if related.Name == parent.Name {
			log.Debugf("not descending into %v to avoid loop", related.Name)
			continue
		}

		var nestedFields []string

		// determine fields in final output handling
		// a. no exported fields                      -> use relationship fields
		// b. exported fields, no relationship fields -> use exported fields
		// c. both relationship and exported fields   -> relfields ∩ exported
		//
		relfields := relationship.Fields
		exported := related.ExportedFields
		reqfields := make([]string, len(requestedFields))
		copy(reqfields, requestedFields)

		if len(exported) == 0 {
			nestedFields = relfields
		} else if len(relfields) == 0 {
			nestedFields = exported
		} else {
			nestedFields = sliceutil.IntersectStrings(relfields, exported)
		}

		// split the nested subfields
		for i, rel := range reqfields {
			if first, last := stringutil.SplitPair(rel, `:`); sliceutil.ContainsString(keys, first) {
				reqfields[i] = last
			} else {
				reqfields[i] = ``
			}
		}

		reqfields = sliceutil.CompactString(reqfields)

		// finally, further constraing the fieldset by those fields being requested
		if len(nestedFields) == 0 {
			nestedFields = reqfields
		} else if len(reqfields) > 0 {
			nestedFields = sliceutil.IntersectStrings(nestedFields, reqfields)
		}

		for _, key := range keys {
			keyBefore, _ := stringutil.SplitPair(key, `.*`)

			if nestedId := record.Get(key); nestedId != nil {
				if typeutil.IsArray(nestedId) {
					results := make([]interface{}, 0)

					for _, id := range sliceutil.Sliceify(nestedId) {
						if prepId != nil {
							id = prepId(id)
						}

						if id != nil {
							results = append(results, &DeferredRecord{
								Original:       nestedId,
								Backend:        backend,
								CollectionName: related.Name,
								ID:             id,
								Keys:           nestedFields,
							})
						} else if !parent.AllowMissingEmbeddedRecords {
							return fmt.Errorf("%v.%v[]: Related record %v.%v is missing", parent.Name, keyBefore, related.Name, nestedId)
						}
					}

					// clear out the array we're modifying
					record.SetNested(keyBefore, []interface{}{})

					for i, result := range results {
						nestKey := strings.Replace(key, `*`, fmt.Sprintf("%d", i), 1)
						record.SetNested(nestKey, result)
						// log.Debugf("%v.%v[%d]: Deferred record %v", parent.Name, key, i, result)
					}

				} else {
					original := nestedId

					if prepId != nil {
						nestedId = prepId(nestedId)
					}

					if nestedId == nil && !parent.AllowMissingEmbeddedRecords {
						return fmt.Errorf("%v.%v: Related record referred to in %v.%v is missing", parent.Name, keyBefore, related.Name, original)
					}

					deferred := &DeferredRecord{
						Original:       nestedId,
						Backend:        backend,
						CollectionName: related.Name,
						ID:             nestedId,
						Keys:           nestedFields,
					}

					record.SetNested(keyBefore, deferred)
					// log.Debugf("%v.%v: Deferred record %v", parent.Name, key, deferred)
				}
			}
		}
	}

	return nil
}

func ResolveDeferredRecords(cache map[string]interface{}, records ...*dal.Record) error {
	deferredRecords := make(map[string][]*DeferredRecord)
	resolvedValues := make([]*recordFieldValue, 0)

	if cache == nil {
		cache = make(map[string]interface{})
	}

	// first pass: get all DeferredRecord values from all records
	//             and map them to each record
	for _, record := range records {
		if err := maputil.Walk(record.Fields, func(value interface{}, key []string, isLeaf bool) error {
			if deferred, ok := value.(DeferredRecord); ok {
				dptr := &deferred
				deferset := deferredRecords[deferred.GroupKey()]
				deferset = append(deferset, dptr)
				deferredRecords[deferred.GroupKey()] = deferset

				resolvedValues = append(resolvedValues, &recordFieldValue{
					Record: record,
					Key:    key,
					Value:  deferred.ID,
				})

				maputil.DeepSet(record.Fields, key, nil)
				return maputil.SkipDescendants
			}

			return nil
		}); err != nil {
			return err
		}
	}

	// second pass:
	// 1. go through the deferred values (already grouped by backend:collection:fieldset)
	// 2. do a bulk retrieve of all the values IDs in each group
	// 3. put the results into the cache map (keyed on backend:collection:ID:fieldset)
	//
	for _, collectionDefers := range deferredRecords {
		if len(collectionDefers) > 0 {
			var keyFn = collectionDefers[0].GroupKey
			var backend = collectionDefers[0].Backend
			var relatedCollectionName = collectionDefers[0].CollectionName
			var fields = collectionDefers[0].Keys
			var ids []interface{}

			// gather all the ids for this deferred collection
			for _, deferred := range collectionDefers {
				ids = append(ids, deferred.ID)
			}

			ids = sliceutil.Unique(ids)

			if len(ids) > 0 {
				if related, err := backend.GetCollection(relatedCollectionName); err == nil {
					indexer := backend.WithSearch(related)

					if indexer == nil {
						// no indexer: need to manually retrieve each record
						for _, id := range ids {
							key := keyFn(id)

							if _, ok := cache[key]; ok {
								continue
							} else if record, err := backend.Retrieve(relatedCollectionName, id, fields...); err == nil {
								cache[key] = record.Fields
							} else {
								return err
							}
						}
					} else {
						bulkQuery := filter.New().AddCriteria(filter.Criterion{
							Field:  related.GetIdentityFieldName(),
							Values: ids,
						})

						bulkQuery.Fields = fields
						bulkQuery.Limit = 1048576

						// query all items at once
						if recordset, err := indexer.Query(related, bulkQuery); err == nil {
							for _, relatedRecord := range recordset.Records {
								key := keyFn(relatedRecord.ID)

								if _, ok := cache[key]; ok {
									continue
								} else {
									cache[key] = relatedRecord.Fields
								}
							}
						} else {
							return err
						}
					}
				} else {
					return err
				}

				// replace each deferred record field with the now-populated related data item
				for _, field := range resolvedValues {
					key := keyFn(field.Value)

					if data, ok := cache[key].(map[string]interface{}); ok {
						maputil.DeepSet(field.Record.Fields, field.Key, data)
					}
				}
			}
		}
	}

	return nil
}
