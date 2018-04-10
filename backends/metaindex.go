package backends

import (
	"fmt"
	"sync"

	"github.com/deckarep/golang-set"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type MetaIndex struct {
	leftIndexer     Indexer
	leftCollection  *dal.Collection
	leftField       string
	rightIndexer    Indexer
	rightCollection *dal.Collection
	rightField      string
}

func NewMetaIndex(leftIndexer Indexer, leftCollection *dal.Collection, leftField string, rightIndexer Indexer, rightCollection *dal.Collection, rightField string) *MetaIndex {
	return &MetaIndex{
		leftIndexer:     leftIndexer,
		leftCollection:  leftCollection,
		leftField:       leftField,
		rightIndexer:    rightIndexer,
		rightCollection: rightCollection,
		rightField:      rightField,
	}
}

func (self *MetaIndex) IndexConnectionString() *dal.ConnectionString {
	return self.leftIndexer.IndexConnectionString()
}

func (self *MetaIndex) IndexInitialize(_ Backend) error {
	return nil
}

func (self *MetaIndex) IndexExists(collection *dal.Collection, id interface{}) bool {
	return false
}

func (self *MetaIndex) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	return nil, fmt.Errorf("MetaIndex only supports querying")
}

func (self *MetaIndex) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	return fmt.Errorf("MetaIndex only supports querying")
}

func (self *MetaIndex) Index(collection *dal.Collection, records *dal.RecordSet) error {
	return fmt.Errorf("MetaIndex only supports querying")
}

func (self *MetaIndex) QueryFunc(collection *dal.Collection, f *filter.Filter, resultFn IndexResultFunc) error {
	leftResults := dal.NewRecordSet()
	uniqueLeftHandValues := mapset.NewSet()

	if f == nil {
		f = filter.All()
	}

	finalFields := f.Fields
	f.Fields = nil
	f.Options[`ForceIndexRecord`] = true

	// perform query on left side, collect all results
	if err := self.leftIndexer.QueryFunc(self.leftCollection, f, func(record *dal.Record, err error, page IndexPage) error {
		if err == nil {
			leftResults.Push(record)
			uniqueLeftHandValues.Add(record.Get(self.leftField))
		}

		return err
	}); err != nil {
		return fmt.Errorf("left-hand index error: %v", err)
	}

	if rightFilter, err := filter.FromMap(map[string]interface{}{
		self.rightField: uniqueLeftHandValues.ToSlice(),
	}); err == nil {
		var leftRecordIndex sync.Map

		rightFilter.Limit = 2147483647

		if err := self.rightIndexer.QueryFunc(self.rightCollection, rightFilter, func(rightRecord *dal.Record, err error, page IndexPage) error {
			if err == nil {
				sharedId := rightRecord.Get(self.rightField)
				var leftRecordIds []int

				// load left-hand records into a map, indexing their positions by ID
				// to allow for O(1) accesses later on
				if ids, ok := leftRecordIndex.Load(sharedId); ok {
					leftRecordIds = ids.([]int)
				} else {
					for i, leftRecord := range leftResults.Records {
						if leftRecord.Get(self.leftField) == sharedId {
							leftRecordIds = append(leftRecordIds, i)
						}
					}

					leftRecordIndex.Store(sharedId, leftRecordIds)
				}

				// for each left-record...
				for _, lrid := range leftRecordIds {
					if leftRecord, ok := leftResults.GetRecord(lrid); ok {
						syntheticRecord := dal.NewRecord([]interface{}{
							leftRecord.ID,
							rightRecord.ID,
						})

						var leftFields = make(map[string]interface{})
						var rightFields = make(map[string]interface{})

						if len(finalFields) > 0 {
							for _, pair := range finalFields {
								cname, field := stringutil.SplitPairTrailing(pair, `.`)

								switch cname {
								case self.leftCollection.Name:
									leftFields[field] = leftRecord.Get(field)

								case self.rightCollection.Name:
									rightFields[field] = rightRecord.Get(field)

								default:
									leftFields[field] = leftRecord.Get(field)
									rightFields[field] = rightRecord.Get(field)
								}
							}
						} else {
							leftFields = leftRecord.Fields
							rightFields = rightRecord.Fields
						}

						syntheticRecord.Set(self.leftCollection.Name, leftFields)

						var rightName string

						if self.leftCollection.Name == self.rightCollection.Name {
							rightName = fmt.Sprintf("%s_right", self.rightCollection.Name)
						} else {
							rightName = self.rightCollection.Name
						}

						syntheticRecord.Set(rightName, rightFields)

						if err := resultFn(syntheticRecord, nil, IndexPage{}); err != nil {
							log.Error(err)
							return err
						}
					} else {
						log.Error(fmt.Errorf("Non-existent left-side record index %v", lrid))
						return fmt.Errorf("Non-existent left-side record index %v", lrid)
					}
				}
			}

			return nil
		}); err != nil {
			return fmt.Errorf("right-hand index error: %v", err)
		}
	} else {
		return fmt.Errorf("right-hand filter error: %v", err)
	}

	return nil
}

func (self *MetaIndex) Query(collection *dal.Collection, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	if f.IdentityField == `` {
		f.IdentityField = ElasticsearchIdentityField
	}

	return DefaultQueryImplementation(self, collection, f, resultFns...)
}

func (self *MetaIndex) ListValues(collection *dal.Collection, fields []string, filter *filter.Filter) (map[string][]interface{}, error) {
	// return self.leftIndexer.ListValues()
	return nil, fmt.Errorf(`Not Implemented`)
}

func (self *MetaIndex) DeleteQuery(collection *dal.Collection, f *filter.Filter) error {
	return fmt.Errorf("MetaIndex only supports querying")
}

func (self *MetaIndex) FlushIndex() error {
	return nil
}

func (self *MetaIndex) GetBackend() Backend {
	return self.leftIndexer.GetBackend()
}

// /api/collections/users.id+teams.user_id/where/
