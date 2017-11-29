package backends

import (
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/guregu/dynamo"
)

var DefaultAmazonRegion = `us-east-1`

type DynamoBackend struct {
	Backend
	Indexer
	cs         dal.ConnectionString
	db         *dynamo.DB
	region     string
	tableCache sync.Map
	indexer    Indexer
}

type dynamoQueryIntent int

const (
	dynamoGetQuery dynamoQueryIntent = iota
	dynamoScanQuery
	dynamoPutQuery
)

func NewDynamoBackend(connection dal.ConnectionString) Backend {
	return &DynamoBackend{
		cs:     connection,
		region: sliceutil.OrString(connection.Host(), DefaultAmazonRegion),
	}
}

func (self *DynamoBackend) GetConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *DynamoBackend) SetIndexer(indexConnString dal.ConnectionString) error {
	if indexer, err := MakeIndexer(indexConnString); err == nil {
		self.indexer = indexer
		return nil
	} else {
		return err
	}
}

func (self *DynamoBackend) Initialize() error {
	var cred *credentials.Credentials

	if u, p, ok := self.cs.Credentials(); ok {
		cred = credentials.NewStaticCredentials(u, p, self.cs.OptString(`token`, ``))
	} else {
		cred = credentials.NewEnvCredentials()
	}

	if ak, err := cred.Get(); err == nil {
		log.Debugf("%T: Access Key ID: %v", self, ak.AccessKeyID)
	} else {
		log.Debugf("%T: failed to retrieve credentials: %v", self, err)
	}

	var logLevel aws.LogLevelType

	if self.cs.OptBool(`debug`, false) {
		logLevel = aws.LogDebugWithHTTPBody
	}

	self.db = dynamo.New(
		session.New(),
		&aws.Config{
			Region:      aws.String(self.region),
			Credentials: cred,
			LogLevel:    &logLevel,
		},
	)

	// retrieve each table once as a cache warming mechanism
	if tables, err := self.db.ListTables().All(); err == nil {
		for _, tableName := range tables {
			if _, err := self.GetCollection(tableName); err != nil {
				return err
			}
		}
	} else {
		return err
	}

	// if self.indexer == nil {
	// 	self.indexer = self
	// }

	if self.indexer != nil {
		if err := self.indexer.IndexInitialize(self); err != nil {
			return err
		}
	}

	return nil
}

func (self *DynamoBackend) RegisterCollection(definition *dal.Collection) {
	self.tableCache.Store(definition.Name, definition)
}

func (self *DynamoBackend) Exists(name string, id interface{}) bool {
	if _, query, err := self.getSingleRecordQuery(name, id); err == nil {
		if n, err := query.Count(); err == nil && n > 0 {
			return true
		}
	}

	return false
}

func (self *DynamoBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if flt, query, err := self.getSingleRecordQuery(name, id, fields...); err == nil {
		output := make(map[string]interface{})

		if err := query.One(&output); err == nil {
			record := dal.NewRecord(output[flt.IdentityField])
			delete(output, flt.IdentityField)
			record.SetFields(output)

			return record, nil
		} else if err == dynamo.ErrNotFound {
			return nil, fmt.Errorf("Record %v does not exist", id)
		} else if err == dynamo.ErrTooMany {
			return nil, fmt.Errorf("Too many records found for ID %v", id)
		} else {
			return nil, fmt.Errorf("query error: %v", err)
		}
	} else {
		return nil, err
	}
}

func (self *DynamoBackend) Insert(name string, records *dal.RecordSet) error {
	if collection, err := self.GetCollection(name); err == nil {
		return self.upsertRecords(collection, records, true)
	} else {
		return err
	}
}

func (self *DynamoBackend) Update(name string, records *dal.RecordSet, target ...string) error {
	if collection, err := self.GetCollection(name); err == nil {
		return self.upsertRecords(collection, records, false)
	} else {
		return err
	}
}

func (self *DynamoBackend) Delete(name string, ids ...interface{}) error {
	for _, id := range ids {
		if op, err := self.getSingleRecordDelete(name, id); err == nil {
			op.Run()
		} else {
			return err
		}
	}

	return nil
}

func (self *DynamoBackend) CreateCollection(definition *dal.Collection) error {
	return fmt.Errorf("Not Implemented")
}

func (self *DynamoBackend) DeleteCollection(name string) error {
	if _, err := self.GetCollection(name); err == nil {
		if err := self.db.Table(name).DeleteTable().Run(); err == nil {
			self.tableCache.Delete(name)
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *DynamoBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(self.tableCache), nil
}

func (self *DynamoBackend) GetCollection(name string) (*dal.Collection, error) {
	return self.cacheTable(name)
}

func (self *DynamoBackend) WithSearch(name string, filters ...*filter.Filter) Indexer {
	return self.indexer
}

func (self *DynamoBackend) WithAggregator(name string) Aggregator {
	return nil
}

func (self *DynamoBackend) Flush() error {
	if self.indexer != nil {
		return self.indexer.FlushIndex()
	}

	return nil
}

func (self *DynamoBackend) toDalType(t dynamo.KeyType) dal.Type {
	switch t {
	case dynamo.BinaryType:
		return dal.RawType
	case dynamo.NumberType:
		return dal.FloatType
	default:
		return dal.StringType
	}
}

func (self *DynamoBackend) cacheTable(name string) (*dal.Collection, error) {
	if table, err := self.db.Table(name).Describe().Run(); err == nil {
		if collectionI, ok := self.tableCache.Load(name); ok {
			return collectionI.(*dal.Collection), nil
		} else {
			collection := &dal.Collection{
				Name:              table.Name,
				IdentityField:     table.HashKey,
				IdentityFieldType: self.toDalType(table.HashKeyType),
			}

			collection.AddFields(dal.Field{
				Name:     table.HashKey,
				Identity: true,
				Key:      true,
				Required: true,
				Type:     self.toDalType(table.HashKeyType),
			})

			if rangeKey := table.RangeKey; rangeKey != `` {
				collection.AddFields(dal.Field{
					Name:     rangeKey,
					Key:      true,
					Required: true,
					Type:     self.toDalType(table.RangeKeyType),
				})
			}

			self.tableCache.Store(name, collection)
			return collection, nil
		}
	} else {
		return nil, err
	}
}

func (self *DynamoBackend) toRecordKeyFilter(collection *dal.Collection, id interface{}, allowMissingRangeKey bool) (*filter.Filter, *dal.Field, error) {
	var rangeKey *dal.Field
	var hashValue interface{}
	var rangeValue interface{}

	for _, field := range collection.Fields {
		if f := field; f.Key {
			rangeKey = &f
		}
	}

	// at least the identity field must have been found
	if collection.IdentityField == `` {
		return nil, nil, fmt.Errorf("No identity field found in collection %v", collection.Name)
	}

	flt := filter.New()
	flt.Limit = 1
	flt.IdentityField = collection.IdentityField

	// if the rangeKey exists, then the id value must be a slice/array containing both parts
	if typeutil.IsArray(id) {
		if v, ok := sliceutil.At(id, 0); ok && v != nil {
			hashValue = v
		}
	} else {
		hashValue = id
	}

	if hashValue != nil {
		flt.AddCriteria(filter.Criterion{
			Field:  collection.IdentityField,
			Type:   collection.IdentityFieldType,
			Values: []interface{}{hashValue},
		})

		if rangeKey != nil {
			if typeutil.IsArray(id) {
				if v, ok := sliceutil.At(id, 1); ok && v != nil {
					rangeValue = v
				}

				flt.AddCriteria(filter.Criterion{
					Type:   rangeKey.Type,
					Field:  rangeKey.Name,
					Values: []interface{}{rangeValue},
				})

			} else if !allowMissingRangeKey {
				return nil, nil, fmt.Errorf("Second ID component must not be nil")
			}
		}

		return flt, rangeKey, nil
	} else {
		return nil, nil, fmt.Errorf("First ID component must not be nil")
	}
}

func (self *DynamoBackend) getSingleRecordQuery(name string, id interface{}, fields ...string) (*filter.Filter, *dynamo.Query, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if flt, rangeKey, err := self.toRecordKeyFilter(collection, id, false); err == nil {
			if hashKeyValue, ok := flt.GetIdentityValue(); ok {
				query := self.db.Table(collection.Name).Get(flt.IdentityField, hashKeyValue)

				query.Consistent(self.cs.OptBool(`readsConsistent`, true))
				query.Limit(int64(flt.Limit))

				if rangeKey != nil {
					if rV, ok := flt.GetValues(rangeKey.Name); ok {
						query.Range(rangeKey.Name, dynamo.Equal, rV...)
					} else {
						return nil, nil, fmt.Errorf("Could not determine range key value")
					}
				}

				return flt, query, nil
			} else {
				return nil, nil, fmt.Errorf("Could not determine hash key value")
			}
		} else {
			return nil, nil, fmt.Errorf("filter create error: %v", err)
		}
	} else {
		return nil, nil, err
	}
}

func (self *DynamoBackend) getSingleRecordDelete(name string, id interface{}) (*dynamo.Delete, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if flt, rangeKey, err := self.toRecordKeyFilter(collection, id, false); err == nil {
			if hashKeyValue, ok := flt.GetIdentityValue(); ok {
				deleteOp := self.db.Table(collection.Name).Delete(flt.IdentityField, hashKeyValue)

				if rangeKey != nil {
					if rV, ok := flt.GetValues(rangeKey.Name); ok && len(rV) > 0 {
						deleteOp.Range(rangeKey.Name, rV[0])
					} else {
						return nil, fmt.Errorf("Could not determine range key value")
					}
				}

				return deleteOp, nil
			} else {
				return nil, fmt.Errorf("Could not determine hash key value")
			}
		} else {
			return nil, fmt.Errorf("filter create error: %v", err)
		}
	} else {
		return nil, err
	}
}

func (self *DynamoBackend) upsertRecords(collection *dal.Collection, records *dal.RecordSet, isCreate bool) error {
	for _, record := range records.Records {
		item := make(map[string]interface{})

		for k, v := range record.Fields {
			item[k] = v
		}

		item[collection.IdentityField] = record.ID

		op := self.db.Table(collection.Name).Put(item)

		if isCreate {
			expr := []string{`attribute_not_exists($)`}

			exprValues := []interface{}{record.ID}

			if rangeKey, ok := collection.GetFirstNonIdentityKeyField(); ok {
				expr = append(expr, `attribute_not_exists($)`)

				if v := record.Get(rangeKey.Name); v != nil {
					exprValues = append(exprValues, v)
				} else {
					return fmt.Errorf("Cannot create record: missing range key")
				}
			}

			op.If(strings.Join(expr, ` AND `), exprValues...)
		}

		if err := op.Run(); err != nil {
			return err
		}
	}

	return nil
}
