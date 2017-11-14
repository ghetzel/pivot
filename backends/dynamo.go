package backends

import (
	"fmt"
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

func (self *DynamoBackend) SetOptions(ConnectOptions) {
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

func (self *DynamoBackend) Insert(collection string, records *dal.RecordSet) error {
	return fmt.Errorf("NI")
}

func (self *DynamoBackend) Update(collection string, records *dal.RecordSet, target ...string) error {
	return fmt.Errorf("NI")
}

func (self *DynamoBackend) Delete(collection string, ids ...interface{}) error {
	return fmt.Errorf("NI")
}

func (self *DynamoBackend) CreateCollection(definition *dal.Collection) error {
	return fmt.Errorf("NI")
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

func (self *DynamoBackend) WithSearch(collection string, filters ...filter.Filter) Indexer {
	return nil
}

func (self *DynamoBackend) WithAggregator(collection string) Aggregator {
	return nil
}

func (self *DynamoBackend) Flush() error {
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
				Name: table.Name,
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
					Identity: false,
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

func (self *DynamoBackend) toRecordKeyFilter(collection *dal.Collection, id interface{}, allowMissingRangeKey bool) (*filter.Filter, *dal.Field, *dal.Field, error) {
	var hashKey *dal.Field
	var rangeKey *dal.Field
	var hashValue interface{}
	var rangeValue interface{}

	for _, field := range collection.Fields {
		if f := field; f.Key {
			if f.Identity {
				hashKey = &f
			} else {
				rangeKey = &f
			}
		}
	}

	// at least the identity field must have been found
	if hashKey == nil {
		return nil, nil, nil, fmt.Errorf("No identity field found in collection %v", collection.Name)
	}

	flt := filter.New()
	flt.Limit = 1
	flt.IdentityField = hashKey.Name

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
			Type:   hashKey.Type,
			Field:  hashKey.Name,
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
				return nil, nil, nil, fmt.Errorf("Second ID component must not be nil")
			}
		}

		return flt, hashKey, rangeKey, nil
	} else {
		return nil, nil, nil, fmt.Errorf("First ID component must not be nil")
	}
}

func (self *DynamoBackend) getSingleRecordQuery(name string, id interface{}, fields ...string) (*filter.Filter, *dynamo.Query, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if flt, _, rangeKey, err := self.toRecordKeyFilter(collection, id, false); err == nil {
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
