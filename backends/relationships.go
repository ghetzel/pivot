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
	Original          interface{}
	Backend           Backend
	IdentityFieldName string
	CollectionName    string
	ID                interface{}
	Keys              []string
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

func PopulateRelationships(backend Backend, parent *dal.Collection, record *dal.Record, prepId func(interface{}) interface{}, requestedFields ...string) error { // for each relationship
	skipKeys := make([]string, 0)

	if embed, ok := backend.(*EmbeddedRecordBackend); ok {
		skipKeys = embed.SkipKeys
	}

	embeds := parent.EmbeddedCollections

	// loop through all the constraints, finding constraints on collections that we
	// don't already have an explicitly-defined embed for.
	//
	// these constraints are addded as implicitly-defined embeds (unless otherwise specified)
	//
ConstraintsLoop:
	for _, constraint := range parent.Constraints {
		if constraint.NoEmbed {
			// don't add this as an implicit embed
			continue
		}

		if err := constraint.Validate(); err == nil {
			for _, embed := range embeds {
				if embed.RelatedCollectionName() == constraint.Collection {
					continue ConstraintsLoop
				}
			}

			embeds = append(embeds, dal.Relationship{
				Keys:           constraint.On,
				CollectionName: constraint.Collection,
			})
		} else {
			return fmt.Errorf("Cannot embed collection: %v", err)
		}
	}

	for _, relationship := range embeds {
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

		if related.Name == parent.Name && !relationship.Force {
			log.Warningf("not embedding records from %q to avoid loop", related.Name)
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

		// finally, further constraint the fieldset by those fields being requested
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
								Original:          nestedId,
								Backend:           backend,
								IdentityFieldName: related.GetIdentityFieldName(),
								CollectionName:    related.Name,
								ID:                id,
								Keys:              nestedFields,
							})
						} else if parent.AllowMissingEmbeddedRecords {
							results = append(results, &DeferredRecord{
								Original:          nestedId,
								Backend:           backend,
								IdentityFieldName: related.GetIdentityFieldName(),
								CollectionName:    related.Name,
								ID:                id,
								Keys:              nestedFields,
							})
						} else {
							return fmt.Errorf("%v.%v[]: Related record %v.%v is missing", parent.Name, keyBefore, related.Name, nestedId)
						}
					}

					// clear out the array we're modifying
					// record.SetNested(keyBefore, []interface{}{})

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
						Original:          nestedId,
						Backend:           backend,
						IdentityFieldName: related.GetIdentityFieldName(),
						CollectionName:    related.Name,
						ID:                nestedId,
						Keys:              nestedFields,
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
	deferredRecords := make(map[string]*DeferredRecord)
	resolvedValues := make([]*recordFieldValue, 0)

	if cache == nil {
		cache = make(map[string]interface{})
	}

	// first pass: get all DeferredRecord values from all records
	//             and map them to each record
	for _, record := range records {
		if err := maputil.WalkStruct(record.Fields, func(value interface{}, key []string, isLeaf bool) error {
			if deferred, ok := value.(DeferredRecord); ok {
				dptr := &deferred
				deferredRecords[deferred.String()] = dptr

				resolvedValues = append(resolvedValues, &recordFieldValue{
					Record:   record,
					Key:      key,
					Value:    deferred.ID,
					Deferred: dptr,
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
	for relatedCollectionName, collectionDefers := range deferredGroupByCollection(deferredRecords) {
		if len(collectionDefers) > 0 {
			var keyFn = collectionDefers[0].GroupKey
			var backend = collectionDefers[0].Backend
			var fields = collectionDefers[0].Keys
			var ids []interface{}

			// gather all the ids for this deferred collection
			for _, deferred := range collectionDefers {
				ids = append(ids, deferred.ID)
			}

			// ids = sliceutil.Unique(ids)
			foundIds := make([]interface{}, 0)

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
								// ensure that the ID always ends up in the related fieldset
								record.Fields[related.IdentityField] = record.ID

								cache[key] = record.Fields
								foundIds = append(foundIds, record.ID)
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
							for _, record := range recordset.Records {
								key := keyFn(record.ID)

								if _, ok := cache[key]; ok {
									continue
								} else {
									// ensure that the ID always ends up in the related fieldset
									record.Fields[related.IdentityField] = record.ID

									cache[key] = record.Fields
									foundIds = append(foundIds, record.ID)
								}
							}
						} else {
							return err
						}
					}
				} else {
					return err
				}

				// figure out which records are missing and sub in a stand-in record
				for _, deferred := range collectionDefers {
					if sliceutil.Contains(foundIds, deferred.ID) {
						continue
					} else {
						key := keyFn(deferred.ID)

						if _, ok := cache[key]; ok {
							continue
						} else {
							cache[key] = map[string]interface{}{
								deferred.IdentityFieldName: deferred.ID,
								`_missing`:                 true,
								`_collection`:              deferred.CollectionName,
							}
						}
					}
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

func deferredGroupByCollection(deferred map[string]*DeferredRecord) map[string][]*DeferredRecord {
	grouped := make(map[string][]*DeferredRecord)

	for _, def := range deferred {
		if group, ok := grouped[def.CollectionName]; ok {
			grouped[def.CollectionName] = append(group, def)
		} else {
			grouped[def.CollectionName] = []*DeferredRecord{def}
		}
	}

	return grouped
}
