package backends

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
)

var ElasticsearchDefaultType = `document`
var ElasticsearchDefaultCompositeJoiner = `:`
var ElasticsearchDefaultScheme = `http`
var ElasticsearchDefaultHost = `localhost:9200`
var ElasticsearchDefaultShards = 3
var ElasticsearchDefaultReplicas = 2

type elasticsearchErrorCause struct {
	Type         string `json:"type"`
	Reason       string `json:"reason"`
	ResourceType string `json:"resource.type"`
	ResourceID   string `json:"resource.id"`
	IndexUUID    string `json:"index_uuid"`
	Index        string `json:"index"`
}

type elasticsearchErrorDetail struct {
	elasticsearchErrorCause
	RootCause []elasticsearchErrorCause `json:"root_cause"`
}

type elasticsearchError struct {
	StatusCode int                      `json:"status"`
	Detail     elasticsearchErrorDetail `json:"error"`
}

func (self *elasticsearchError) Error() string {
	return self.Detail.Reason
}

type elasticsearchIndexSettings struct {
	NumberOfShards   int `json:"number_of_shards"`
	NumberOfReplicas int `json:"number_of_replicas"`
}

type elasticsearchIndexMappings struct {
	Properties map[string]interface{} `json:"properties"`
}

type elasticsearchCreateIndex struct {
	Settings elasticsearchIndexSettings `json:"settings"`
	Mappings elasticsearchIndexMappings `json:"mappings"`
}

type ElasticsearchBackend struct {
	cs          dal.ConnectionString
	indexer     Indexer
	client      *httputil.Client
	tableCache  sync.Map
	docType     string
	pkSeparator string
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
		fmt.Sprintf(
			"%s://%s",
			self.cs.Protocol(ElasticsearchDefaultScheme),
			self.cs.Host(ElasticsearchDefaultHost),
		),
	); err == nil {
		self.client = client
		self.client.SetHeader(`Content-Type`, `application/json`)
		self.client.SetHeader(`Accept`, `identity`)
		self.client.SetErrorDecoder(func(res *http.Response) error {
			var body io.ReadCloser = res.Body

			if body != nil {
				var eserr elasticsearchError

				if err := json.NewDecoder(body).Decode(&eserr); err == nil {
					return &eserr
				} else {
					return err
				}
			} else {
				return nil
			}
		})
	} else {
		return err
	}

	// specify explicitly-provided credentials first
	if u, p, ok := self.cs.Credentials(); ok {
		self.client.SetBasicAuth(u, p)
	}

	self.client.SetInsecureTLS(self.cs.Opt(`insecure`).Bool())
	self.docType = self.cs.OptString(`type`, ElasticsearchDefaultType)
	self.pkSeparator = self.cs.OptString(`joiner`, ElasticsearchDefaultCompositeJoiner)

	if self.cs.OptBool(`autoregister`, true) {
		if res, err := self.client.Get(`/_cat/indices`, nil, nil); err == nil {
			if res.Body != nil {
				defer res.Body.Close()

				if data, err := ioutil.ReadAll(res.Body); err == nil {
					for _, line := range stringutil.SplitLines(data, "\n") {
						if parts := strings.FieldsFunc(line, unicode.IsSpace); len(parts) >= 3 {
							if _, err := self.cacheIndex(parts[2]); err != nil {
								return fmt.Errorf("autoregister %v: %v", parts[2], err)
							}
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
	if collection, err := self.GetCollection(name); err == nil {
		if _, err := self.client.Request(
			http.MethodHead,
			fmt.Sprintf(
				"/%s/%s/%v",
				self.esIndexName(collection.Name),
				self.docType,
				self.pk(id),
			),
			nil,
			nil,
			nil,
		); err == nil {
			return true
		}
	}

	return false
}

func (self *ElasticsearchBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if res, err := self.client.Get(
			fmt.Sprintf(
				"/%s/%s/%v",
				self.esIndexName(collection.Name),
				self.docType,
				self.pk(id),
			),
			nil,
			nil,
		); err == nil {
			var doc elasticsearchDocument

			if err := self.client.Decode(res.Body, &doc); err != nil {
				return nil, err
			}

			return dal.NewRecord(doc.ID).SetFields(doc.Source), nil
		} else {
			return nil, err
		}
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
	if collection, err := self.GetCollection(name); err == nil {
		// for each id we're deleting...
		for _, id := range ids {
			if _, err := self.client.Delete(
				fmt.Sprintf(
					"/%s/%s/%v",
					self.esIndexName(collection.Name),
					self.docType,
					self.pk(id),
				),
				nil,
				nil,
			); err != nil {
				return err
			}
		}

		return nil
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) CreateCollection(definition *dal.Collection) error {
	var index = elasticsearchCreateIndex{
		Settings: elasticsearchIndexSettings{
			NumberOfShards:   ElasticsearchDefaultShards,
			NumberOfReplicas: ElasticsearchDefaultReplicas,
		},
		Mappings: elasticsearchIndexMappings{
			Properties: make(map[string]interface{}),
		},
	}

	for _, field := range definition.Fields {
		var estype string

		switch field.Type {
		case dal.BooleanType:
			estype = `boolean`
		case dal.IntType:
			estype = `long`
		case dal.FloatType:
			estype = `double`
		case dal.TimeType:
			estype = `date`
		case dal.ObjectType:
			estype = `object`
		case dal.ArrayType:
			estype = `nested`
		default:
			estype = `text`
		}

		index.Mappings.Properties[field.Name] = map[string]interface{}{
			`type`: estype,
		}
	}

	if _, err := self.client.Put(
		fmt.Sprintf(
			"/%s",
			self.esIndexName(definition.Name),
		),
		&index,
		nil,
		nil,
	); err == nil {
		self.RegisterCollection(definition)
		return nil
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) DeleteCollection(name string) error {
	if collection, err := self.GetCollection(name); err == nil {
		res, err := self.client.Delete(`/`+self.esIndexName(collection.Name), nil, nil)
		self.tableCache.Delete(collection.Name)

		if res.StatusCode == 404 {
			return dal.CollectionNotFound
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(&self.tableCache), nil
}

func (self *ElasticsearchBackend) GetCollection(name string) (*dal.Collection, error) {
	return self.cacheIndex(name)
}

func (self *ElasticsearchBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
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

func (self *ElasticsearchBackend) pk(id interface{}) string {
	if typeutil.IsArray(id) {
		return strings.Join(sliceutil.Stringify(id), self.pkSeparator)
	} else {
		return typeutil.String(id)
	}
}

func (self *ElasticsearchBackend) cacheIndex(name string) (*dal.Collection, error) {
	if collectionI, ok := self.tableCache.Load(name); ok {
		return collectionI.(*dal.Collection), nil
	}

	var collection = dal.NewCollection(name)

	//TODO: parse existing mapping and work out fields

	self.tableCache.Store(name, collection)

	return collection, nil
}

func (self *ElasticsearchBackend) esIndexName(collectionName string) string {
	return stringutil.Underscore(collectionName)
}

func (self *ElasticsearchBackend) upsertRecords(collection *dal.Collection, records *dal.RecordSet, isCreate bool) error {
	for _, record := range records.Records {
		var body = record.Map()
		var pk = self.pk(record.Keys(collection))

		delete(body, `id`)
		body[`_id`] = pk

		if _, err := self.client.Post(
			fmt.Sprintf(
				"/%s/%s/%v",
				collection.Name,
				self.docType,
				pk,
			),
			body,
			nil,
			nil,
		); err != nil {
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
