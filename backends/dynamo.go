package backends

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/guregu/dynamo"
)

var DefaultAmazonRegion = `us-east-1`

type DynamoBackend struct {
	Backend
	Indexer
	cs     dal.ConnectionString
	db     *dynamo.DB
	region string
	tables []string
}

func NewDynamoBackend(connection dal.ConnectionString) Backend {
	return &DynamoBackend{
		cs:     connection,
		region: DefaultAmazonRegion,
	}
}

func (self *DynamoBackend) GetConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *DynamoBackend) SetOptions(options ConnectOptions) {
	if region := options.Region; region != `` {
		self.region = region
	}
}

func (self *DynamoBackend) Initialize() error {
	self.db = dynamo.New(
		session.New(),
		&aws.Config{
			Region: aws.String(self.region),
		},
	)
}

func (self *DynamoBackend) RegisterCollection(*dal.Collection) {

}

func (self *DynamoBackend) Exists(collection string, id interface{}) bool {
	return false
}

func (self *DynamoBackend) Retrieve(collection string, id interface{}, fields ...string) (*dal.Record, error) {
	return nil, fmt.Errorf("NI")
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

func (self *DynamoBackend) DeleteCollection(collection string) error {
	return fmt.Errorf("NI")
}

func (self *DynamoBackend) ListCollections() ([]string, error) {
	return self.db.ListTables().All()
}

func (self *DynamoBackend) GetCollection(name string) (*dal.Collection, error) {
	if collectionI, ok := self.tableCache.Load(name); ok {
		return collectionI.(*dal.Collection), nil
	} else {
		if table, err := self.db.Table(name).Describe().Run(); err == nil {
			collection := &dal.Collection{
				Name: table.Name,
			}

			collection.AddFields(dal.Field{
				Name:     table.HashKey,
				Identity: true,
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
		} else {
			return nil, err
		}
	}
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
