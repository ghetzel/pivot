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

var ElasticsearchDefaultType = `_doc`
var ElasticsearchDefaultCompositeJoiner = `--`
var ElasticsearchDefaultScheme = `http`
var ElasticsearchDefaultHost = `localhost:9200`
var ElasticsearchDefaultShards = 3
var ElasticsearchDefaultReplicas = 2
var ElasticsearchDefaultRefresh = `false`
var ElasticsearchAnalyzers = map[string]interface{}{
	`pivot_case_insensitive`: map[string]interface{}{
		`tokenizer`: `keyword`,
		`filter`:    []string{`lowercase`},
	},
}

var ElasticsearchNormalizers = map[string]interface{}{
	`pivot_normalize_string`: map[string]interface{}{
		`type`:   `custom`,
		`filter`: []string{`lowercase`},
	},
}

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
	StatusCode int                       `json:"status"`
	Detail     *elasticsearchErrorDetail `json:"error,omitempty"`
}

func (self *elasticsearchError) Error() string {
	if detail := self.Detail; detail != nil {
		return detail.Reason
	} else {
		return fmt.Sprintf("HTTP %d", self.StatusCode)
	}
}

type elasticsearchIndexAnalysis struct {
	Analyzer   map[string]interface{} `json:"analyzer"`
	Normalizer map[string]interface{} `json:"normalizer"`
}

type elasticsearchIndexSettings struct {
	NumberOfShards   int                        `json:"index.number_of_shards"`
	NumberOfReplicas int                        `json:"index.number_of_replicas"`
	Analysis         elasticsearchIndexAnalysis `json:"analysis"`
}

type elasticsearchIndexMappings struct {
	Properties map[string]interface{} `json:"properties"`
	Dyanmic    bool                   `json:"dynamic"`
}

type elasticsearchCreateIndex struct {
	Settings elasticsearchIndexSettings `json:"settings"`
	Mappings elasticsearchIndexMappings `json:"mappings"`
}

type ElasticsearchBackend struct {
	cs              dal.ConnectionString
	indexer         Indexer
	client          *httputil.Client
	tableCache      sync.Map
	docType         string
	pkSeparator     string
	shards          int
	replicas        int
	refresh         string
	iAmMyOwnIndexer bool
}

func NewElasticsearchBackend(connection dal.ConnectionString) Backend {
	return &ElasticsearchBackend{
		cs:       connection,
		shards:   ElasticsearchDefaultShards,
		replicas: ElasticsearchDefaultReplicas,
		refresh:  ElasticsearchDefaultRefresh,
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
		_, err := self.client.Get(`/_cat/health`, nil, nil)
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

		// self.client.SetPreRequestHook(func(req *http.Request) (interface{}, error) {
		// 	log.Debugf("elastic > %s %v", req.Method, req.URL)
		// 	return nil, nil
		// })

		// self.client.SetPostRequestHook(func(res *http.Response, _ interface{}) error {
		// 	log.Debugf("elastic < HTTP %v", res.Status)
		// 	return nil
		// })

		self.client.SetErrorDecoder(esErrorDecoder)
		self.client.SetHeader(`Content-Type`, `application/json`)
		self.client.SetHeader(`Accept-Encoding`, `identity`)
		self.client.SetHeader(`User-Agent`, ClientUserAgent)
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
	self.shards = int(self.cs.OptInt(`shards`, int64(self.shards)))
	self.replicas = int(self.cs.OptInt(`replicas`, int64(self.replicas)))
	self.refresh = self.cs.OptString(`refresh`, self.refresh)

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
		var esi = NewElasticsearchIndexer(self.cs)

		// we're going to use our client in the indexer
		esi.client = self.client
		esi.refresh = self.refresh
		self.indexer = esi
	}

	if self.indexer != nil {
		if err := self.indexer.IndexInitialize(self); err == nil {
			self.iAmMyOwnIndexer = true
		} else {
			return err
		}
	}

	return nil
}

func (self *ElasticsearchBackend) RegisterCollection(definition *dal.Collection) {
	self.tableCache.Store(definition.Name, definition)
}

func (self *ElasticsearchBackend) Exists(name string, id interface{}) bool {
	if pk := self.pk(id); pk != `` {
		if collection, err := self.GetCollection(name); err == nil {
			if _, err := self.client.Request(
				http.MethodHead,
				fmt.Sprintf(
					"/%s/%s/%v",
					collection.Name,
					self.docType,
					pk,
				),
				nil,
				nil,
				nil,
			); err == nil {
				return true
			}
		}
	}

	return false
}

func (self *ElasticsearchBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.GetCollection(name); err == nil {
		if res, err := self.client.Get(
			fmt.Sprintf(
				"/%s/%s/%v",
				collection.Name,
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

			return doc.record(collection, self.pkSeparator)
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
					collection.Name,
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
			NumberOfShards:   self.shards,
			NumberOfReplicas: self.replicas,
			Analysis: elasticsearchIndexAnalysis{
				Analyzer:   ElasticsearchAnalyzers,
				Normalizer: ElasticsearchNormalizers,
			},
		},
		Mappings: elasticsearchIndexMappings{
			Properties: make(map[string]interface{}),
		},
	}

	for _, field := range definition.Fields {
		index.Mappings.Properties[field.Name] = fieldTypeToEsMapping(field.Type)
	}

	index.Mappings.Properties[definition.GetIdentityFieldName()] = fieldTypeToEsMapping(definition.IdentityFieldType)

	if _, err := self.client.Put(
		`/`+definition.Name,
		&index,
		map[string]interface{}{
			`wait_for_active_shards`: 1,
			`include_type_name`:      false,
		},
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
		res, err := self.client.Delete(`/`+collection.Name, nil, nil)
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
	var collection *dal.Collection

	if collectionI, ok := self.tableCache.Load(name); ok {
		if c, ok := collectionI.(*dal.Collection); ok {
			if c.Ready {
				return c, nil
			} else {
				collection = c
			}
		}
	}

	if res, err := self.client.Get(
		`/`+name,
		nil,
		nil,
	); err == nil {
		if collection == nil {
			collection = dal.NewCollection(name)
		}

		//TODO: parse existing mapping and work out fields

		collection.Ready = true
		self.tableCache.Store(name, collection)

		return collection, nil
	} else if res.StatusCode == http.StatusNotFound {
		return nil, dal.CollectionNotFound
	} else {
		return nil, err
	}
}

func (self *ElasticsearchBackend) upsertRecords(collection *dal.Collection, records *dal.RecordSet, isCreate bool) error {
	for _, record := range records.Records {
		if r, err := collection.StructToRecord(record); err == nil {
			record = r
		} else {
			return err
		}

		var pk = self.pk(record.Keys(collection))

		delete(record.Fields, collection.GetIdentityFieldName())
		delete(record.Fields, `_id`)

		if payload, err := collection.MapFromRecord(record); err == nil {
			if _, err := self.client.Post(
				fmt.Sprintf(
					"/%s/%s/%v",
					collection.Name,
					self.docType,
					pk,
				),
				payload,
				map[string]interface{}{
					`refresh`: self.refresh,
				},
				nil,
			); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if !collection.SkipIndexPersistence && !self.iAmMyOwnIndexer {
		if search := self.WithSearch(collection); search != nil {
			if err := search.Index(collection, records); err != nil {
				return err
			}
		}
	}

	return nil
}

func esErrorDecoder(res *http.Response) error {
	if res != nil {
		var body io.ReadCloser = res.Body

		if body != nil {
			var eserr elasticsearchError

			if err := json.NewDecoder(body).Decode(&eserr); err == nil {
				if eserr.StatusCode > 0 {
					return &eserr
				}
			} else {
				return fmt.Errorf("elastic error decode: %v", err)
			}
		}
	}

	return nil
}

func fieldTypeToEsMapping(ftype dal.Type) map[string]interface{} {
	switch ftype {
	case dal.BooleanType:
		return map[string]interface{}{
			`type`: `boolean`,
		}
	case dal.IntType:
		return map[string]interface{}{
			`type`: `long`,
		}
	case dal.FloatType:
		return map[string]interface{}{
			`type`: `double`,
		}
	case dal.TimeType:
		return map[string]interface{}{
			`type`: `date`,
		}
	case dal.ObjectType:
		return map[string]interface{}{
			`type`:    `object`,
			`dynamic`: true,
		}
	case dal.ArrayType:
		return map[string]interface{}{
			`type`: `nested`,
		}
	default:
		return map[string]interface{}{
			`type`:       `keyword`,
			`normalizer`: `pivot_normalize_string`,
		}
	}
}
