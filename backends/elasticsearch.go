package backends

import (
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

type ElasticsearchBackend struct {
	cs         dal.ConnectionString
	indexer    Indexer
	client     *httputil.Client
	tableCache sync.Map
}

func NewElasticsearchBackend(connection dal.ConnectionString) Backend {
	return &ElasticsearchBackend{
		cs: connection,
	}
}

func (self *ElasticsearchBackend) Supports(features ...BackendFeature) bool {
	for _, feat := range features {
		switch feat {
		case CompositeKeys:
			continue
		default:
			return false
		}
	}

	return true
}

func (self *ElasticsearchBackend) String() string {
	return `elasticsearch`
}

func (self *ElasticsearchBackend) GetConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *ElasticsearchBackend) Ping(timeout time.Duration) error {
	if self.client == nil {
		return fmt.Errorf("Backend not initialized")
	} else {
		_, err := self.client.Get(`/`, nil, nil)
		return err
	}
}

func (self *ElasticsearchBackend) SetIndexer(indexConnString dal.ConnectionString) error {
	if indexer, err := MakeIndexer(indexConnString); err == nil {
		self.indexer = indexer
		return nil
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) Initialize() error {
	if client, err := httputil.NewClient(
		fmt.Sprintf("%s://%s", self.cs.Protocol(`http`), self.cs.Host()),
	); err == nil {
		self.client = client
	} else {
		return err
	}

	// specify explicitly-provided credentials first
	if u, p, ok := self.cs.Credentials(); ok {
		self.client.SetBasicAuth(u, p)
	}

	self.client.SetInsecureTLS(self.cs.Opt(`insecure`).Bool())

	if self.cs.OptBool(`autoregister`, true) {
		if res, err := self.client.Get(`/_cat/indices`, nil, nil); err == nil {
			if res.Body != nil {
				defer res.Body.Close()

				if data, err := ioutil.ReadAll(res.Body); err == nil {
					for _, line := range stringutil.SplitLines(data, "\n") {
						if parts := strings.FieldsFunc(line, unicode.IsSpace); len(parts) >= 3 {
							self.cacheIndex
						}
					}
				} else {
					return fmt.Errorf("indices: %v", err)
				}
			}
		} else {
			return fmt.Errorf("indices: %v", err)
		}
	}

	if self.indexer == nil {
		self.indexer = NewElasticsearchIndexer(self.cs)
	}

	if self.indexer != nil {
		if err := self.indexer.IndexInitialize(self); err != nil {
			return err
		}
	}

	return nil
}

func (self *ElasticsearchBackend) RegisterCollection(definition *dal.Collection) {
	self.tableCache.Store(definition.Name, definition)
}

func (self *ElasticsearchBackend) Exists(name string, id interface{}) bool {

}

func (self *ElasticsearchBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.GetCollection(name); err == nil {

	} else {
		return nil, err
	}
}

func (self *ElasticsearchBackend) Insert(name string, records *dal.RecordSet) error {
	if collection, err := self.GetCollection(name); err == nil {
		return self.upsertRecords(collection, records, true)
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) Update(name string, records *dal.RecordSet, target ...string) error {
	if collection, err := self.GetCollection(name); err == nil {
		return self.upsertRecords(collection, records, false)
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) Delete(name string, ids ...interface{}) error {
	if _, err := self.GetCollection(name); err == nil {
		// for each id we're deleting...
		for _, id := range ids {

		}

		return nil
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) CreateCollection(definition *dal.Collection) error {
	return fmt.Errorf("Not Implemented")
}

func (self *ElasticsearchBackend) DeleteCollection(name string) error {
	if _, err := self.GetCollection(name); err == nil {

	} else {
		return err
	}
}

func (self *ElasticsearchBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(&self.tableCache), nil
}

func (self *ElasticsearchBackend) GetCollection(name string) (*dal.Collection, error) {
	return self.cacheTable(name)
}

func (self *ElasticsearchBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	// if this is a query we _can_ handle, then use ourself as the indexer
	if len(filters) > 0 {
		if err := self.validateFilter(collection, filters[0]); err == nil {
			return self
		}
	}

	return self.indexer
}

func (self *ElasticsearchBackend) WithAggregator(collection *dal.Collection) Aggregator {
	if self.indexer != nil {
		if agg, ok := self.indexer.(Aggregator); ok {
			return agg
		}
	}

	return nil
}

func (self *ElasticsearchBackend) Flush() error {
	if self.indexer != nil {
		return self.indexer.FlushIndex()
	}

	return nil
}

func (self *ElasticsearchBackend) cacheIndex(name string) (*dal.Collection, error) {
	if collectionI, ok := self.tableCache.Load(name); ok {
		return collectionI.(*dal.Collection), nil
	}

	collection = dal.NewCollection(name)

	self.tableCache.Store(name, collection)

	return collection, nil
}

func (self *ElasticsearchBackend) upsertRecords(collection *dal.Collection, records *dal.RecordSet, isCreate bool) error {
	for _, record := range records.Records {

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
