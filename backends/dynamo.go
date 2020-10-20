package backends

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/ghetzel/go-stockutil/fileutil"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

var DefaultAmazonRegion = `us-east-1`
var DefaultSharedCredentialsFile = `~/.aws/credentials`
var DefaultSharedCredentialsProfile = `default`
var DyanmoListFieldsLimit = 8192

type DynamoBackend struct {
	Backend
	Indexer
	cs         dal.ConnectionString
	db         *dynamodb.DynamoDB
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

func (self *DynamoBackend) Supports(features ...BackendFeature) bool {
	for _, feat := range features {
		switch feat {
		case PartialSearch, CompositeKeys:
			continue
		default:
			return false
		}
	}

	return true
}

func (self *DynamoBackend) String() string {
	return `dynamodb`
}

func (self *DynamoBackend) GetConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *DynamoBackend) Ping(timeout time.Duration) error {
	if self.db == nil {
		return fmt.Errorf("Backend not initialized")
	}

	return nil
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
	var providers []credentials.Provider
	var logLevel aws.LogLevelType

	// specify explicitly-provided credentials first
	if u, p, ok := self.cs.Credentials(); ok {
		providers = append(providers, &credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     u,
				SecretAccessKey: p,
				SessionToken:    self.cs.OptString(`token`, ``),
			},
		})
	}

	// if the shared credentials file exists, use it
	shared := fileutil.MustExpandUser(DefaultSharedCredentialsFile)

	if fileutil.IsNonemptyFile(shared) {
		providers = append(providers, &credentials.SharedCredentialsProvider{
			Filename: shared,
			Profile:  sliceutil.OrString(os.Getenv(`AWS_PROFILE`), DefaultSharedCredentialsProfile),
		})
	}

	// add the environment variables provider last
	providers = append(providers, &credentials.EnvProvider{})

	if self.cs.OptBool(`debug`, false) {
		logLevel = aws.LogDebugWithHTTPBody
	}

	self.db = dynamodb.New(
		session.New(),
		&aws.Config{
			Region:      aws.String(self.region),
			Credentials: credentials.NewChainCredentials(providers),
			LogLevel:    &logLevel,
		},
	)

	if self.cs.OptBool(`autoregister`, true) {
		// retrieve each table once as a cache warming mechanism
		if output, err := self.db.ListTables(&dynamodb.ListTablesInput{
			Limit: aws.Int64(100),
		}); err == nil {
			for _, tableName := range output.TableNames {
				if _, err := self.GetCollection(*tableName); err != nil {
					return err
				}
			}
		} else {
			return err
		}
	}

	if self.indexer == nil {
		self.indexer = self
	}

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
	if _, keys, err := self.getKeyAttributes(name, id); err == nil {
		if out, err := self.db.GetItem(&dynamodb.GetItemInput{
			TableName:      aws.String(name),
			ConsistentRead: aws.Bool(self.cs.OptBool(`readsConsistent`, true)),
			Key:            keys,
		}); err == nil {
			if len(out.Item) > 0 {
				return true
			}
		}
	}

	return false
}

func (self *DynamoBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.GetCollection(name); err == nil {
		// get the key attributes that target this specific record
		if _, keys, err := self.getKeyAttributes(name, id); err == nil {
			// execute the GetItem request
			if out, err := self.db.GetItem(&dynamodb.GetItemInput{
				TableName:      aws.String(name),
				ConsistentRead: aws.Bool(self.cs.OptBool(`readsConsistent`, true)),
				Key:            keys,
			}); err == nil {
				// return the record
				return dynamoRecordFromItem(collection, id, out.Item)
			} else if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeResourceNotFoundException:
					return nil, fmt.Errorf("Record %v does not exist", id)
				case dynamodb.ErrCodeProvisionedThroughputExceededException:
					return nil, fmt.Errorf("Throughput exceeded")
				default:
					return nil, aerr
				}
			} else {
				return nil, err
			}
		} else {
			return nil, err
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
	if _, err := self.GetCollection(name); err == nil {
		// for each id we're deleting...
		for _, id := range ids {
			// get the key attributes that target this specific record
			if _, keys, err := self.getKeyAttributes(name, id); err == nil {
				// execute the DeleteItem request
				if _, err := self.db.DeleteItem(&dynamodb.DeleteItemInput{
					TableName: aws.String(name),
					Key:       keys,
				}); err != nil {
					// handle errors (if any)
					if aerr, ok := err.(awserr.Error); ok {
						switch aerr.Code() {
						case dynamodb.ErrCodeProvisionedThroughputExceededException:
							return fmt.Errorf("Throughput exceeded")
						default:
							return aerr
						}
					} else {
						return err
					}
				}
			} else {
				return err
			}
		}

		return nil
	} else {
		return err
	}
}

func (self *DynamoBackend) CreateCollection(definition *dal.Collection) error {
	if definition.View {
		return fmt.Errorf("View-type collections are not supported on this backend.")
	}

	return fmt.Errorf("Not Implemented")

}

func (self *DynamoBackend) DeleteCollection(name string) error {
	if _, err := self.GetCollection(name); err == nil {
		if _, err := self.db.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(name),
		}); err == nil {
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
	return maputil.StringKeys(&self.tableCache), nil
}

func (self *DynamoBackend) GetCollection(name string) (*dal.Collection, error) {
	return self.cacheTable(name)
}

func (self *DynamoBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	// if this is a query we _can_ handle, then use ourself as the indexer
	if len(filters) > 0 {
		if err := self.validateFilter(collection, filters[0]); err == nil {
			return self
		}
	}

	return self.indexer
}

func (self *DynamoBackend) WithAggregator(collection *dal.Collection) Aggregator {
	if self.indexer != nil {
		if agg, ok := self.indexer.(Aggregator); ok {
			return agg
		}
	}

	return nil
}

func (self *DynamoBackend) Flush() error {
	if self.indexer != nil {
		return self.indexer.FlushIndex()
	}

	return nil
}

func (self *DynamoBackend) toDalType(t string) dal.Type {
	switch t {
	case `BS`:
		return dal.RawType
	case `BOOL`:
		return dal.BooleanType
	case `N`:
		return dal.FloatType
	default:
		return dal.StringType
	}
}

func (self *DynamoBackend) cacheTable(name string) (*dal.Collection, error) {
	if collectionI, ok := self.tableCache.Load(name); ok {
		return collectionI.(*dal.Collection), nil
	} else if table, err := self.db.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	}); err == nil {
		typemap := make(map[string]string)

		for _, def := range table.Table.AttributeDefinitions {
			typemap[*def.AttributeName] = *def.AttributeType
		}

		collection := &dal.Collection{
			Name:              *table.Table.TableName,
			IdentityField:     *table.Table.KeySchema[0].AttributeName,
			IdentityFieldType: self.toDalType(typemap[*table.Table.KeySchema[0].AttributeName]),
		}

		collection.AddFields(dal.Field{
			Name:     collection.IdentityField,
			Identity: true,
			Key:      true,
			Required: true,
			Type:     collection.IdentityFieldType,
		})

		if ks := table.Table.KeySchema; len(ks) > 1 {
			collection.AddFields(dal.Field{
				Name:     *ks[1].AttributeName,
				Key:      true,
				Required: true,
				Type:     self.toDalType(typemap[*ks[1].AttributeName]),
			})
		}

		self.tableCache.Store(name, collection)
		return collection, nil
	} else {
		return nil, err
	}
}

func dynamoRecordFromItem(collection *dal.Collection, id interface{}, item map[string]*dynamodb.AttributeValue) (*dal.Record, error) {
	// build the record we're returning
	var data = make(map[string]interface{})
	var record = dal.NewRecord(nil)

	if err := dynamodbattribute.UnmarshalMap(item, &data); err == nil {
		for k, v := range data {
			if field, ok := collection.GetField(k); ok {
				if typed, err := field.ConvertValue(v); err == nil {
					if collection.IsIdentityField(field.Name) {
						record.ID = typed
					} else if collection.IsKeyField(field.Name) {
						record.Set(k, typed)
					} else {
						record.Set(k, typed)
					}
				} else {
					log.Warningf("Unable to parse value returned from DynamoDB for field %v: %v", k, err)
				}
			} else {
				record.Set(k, v)
			}
		}

		if id != nil {
			var idParts = sliceutil.Sliceify(id)
			var recordKeys = record.Keys(collection)

			// double check that the keys we requested line up with what we got back
			if len(recordKeys) == len(idParts) {
				for i, have := range recordKeys {
					if ok, err := stringutil.RelaxedEqual(have, idParts[i]); err != nil || !ok {
						return nil, fmt.Errorf("Record %v does not exist", id)
					}
				}
			} else {
				return nil, fmt.Errorf("Record %v does not exist", id)
			}
		}

		return record, nil
	} else {
		return record, fmt.Errorf("Cannot unmarshal item to map: %v", err)
	}
}

func dynamoRecordToItem(collection *dal.Collection, record *dal.Record) (map[string]*dynamodb.AttributeValue, error) {
	if data, err := collection.MapFromRecord(record); err == nil {
		return dynamodbattribute.MarshalMap(data)
	} else {
		return nil, err
	}
}

func dynamoToRecordKeyFilter(collection *dal.Collection, id interface{}) (*filter.Filter, *dal.Field, error) {
	var rangeKey *dal.Field
	var hashValue interface{}
	var rangeValue interface{}
	var allowMissingRangeKey bool

	// detect if the collection has a range key (i.e.: sort key)
	if _, ok := collection.GetFirstNonIdentityKeyField(); !ok {
		allowMissingRangeKey = true
	}

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
	if record, ok := id.(*dal.Record); ok {
		hashValue = sliceutil.First(record.ID)
	} else if typeutil.IsArray(id) {
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

func dynamoToDynamoAttributes(collection *dal.Collection, values map[string]interface{}, fieldMap map[string]string) map[string]*dynamodb.AttributeValue {
	rv := make(map[string]*dynamodb.AttributeValue)

	for k, v := range values {
		fieldName := k

		if len(fieldMap) > 0 {
			if mappedName, ok := fieldMap[k]; ok {
				fieldName = mappedName
			}
		}

		if field, ok := collection.GetField(fieldName); ok {
			aval := new(dynamodb.AttributeValue)

			switch field.Type {
			case dal.BooleanType:
				aval = aval.SetBOOL(typeutil.V(v).Bool())
			case dal.FloatType, dal.IntType:
				aval = aval.SetN(typeutil.V(v).String())
			default:
				aval = aval.SetS(typeutil.V(v).String())
			}

			rv[k] = aval
		}
	}

	return rv
}

func (self *DynamoBackend) getKeyAttributes(name string, id interface{}) (*filter.Filter, map[string]*dynamodb.AttributeValue, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if flt, rangeKey, err := dynamoToRecordKeyFilter(collection, id); err == nil {
			if hashKeyValue, ok := flt.GetIdentityValue(); ok {
				keys := map[string]interface{}{
					flt.IdentityField: hashKeyValue,
				}

				if rangeKey != nil {
					if values, ok := flt.GetValues(rangeKey.Name); ok && len(values) == 1 {
						keys[rangeKey.Name] = values[0]
					} else {
						return nil, nil, fmt.Errorf("Could not determine range key value")
					}
				}

				querylog.Debugf("[%v] retrieve: %v %v", self, collection.Name, id)

				return flt, dynamoToDynamoAttributes(collection, keys, nil), nil
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

func (self *DynamoBackend) upsertRecords(collection *dal.Collection, records *dal.RecordSet, isCreate bool) error {
	for _, record := range records.Records {
		if item, err := dynamoRecordToItem(collection, record); err == nil {
			op := &dynamodb.PutItemInput{
				TableName: aws.String(collection.Name),
				Item:      item,
			}

			// if this is a create statement, we need to add conditions to the PutItem call that
			// ensures that an existing record with these id(s) doesn't exist.
			if isCreate {
				expr := []string{`attribute_not_exists(#HashKey)`}
				attrNames := map[string]*string{
					`#HashKey`: aws.String(collection.IdentityField),
				}

				// if there's a range key, we gotta add that to the conditional expression too
				if rangeKey, ok := collection.GetFirstNonIdentityKeyField(); ok {
					expr = append(expr, `attribute_not_exists(#RangeKey)`)
					attrNames[`#RangeKey`] = aws.String(rangeKey.Name)
				}

				op = op.SetConditionExpression(strings.Join(expr, ` AND `))
				op = op.SetExpressionAttributeNames(attrNames)
			}

			// perform the call
			if _, err := self.db.PutItem(op); err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case dynamodb.ErrCodeConditionalCheckFailedException:
						return fmt.Errorf("Record already exists")
					case dynamodb.ErrCodeProvisionedThroughputExceededException:
						return fmt.Errorf("Throughput exceeded")
					default:
						return aerr
					}
				} else {
					return err
				}
			}
		} else {
			return err
		}
	}

	if !collection.SkipIndexPersistence {
		if search := self.WithSearch(collection); search != nil {
			if err := search.Index(collection, records); err != nil {
				return err
			}
		}
	}

	return nil
}
