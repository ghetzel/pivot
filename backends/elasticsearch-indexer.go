package backends

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/go-stockutil/log"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/pivot/v3/dal"
	"github.com/ghetzel/pivot/v3/filter"
	"github.com/ghetzel/pivot/v3/filter/generators"
)

var ElasticsearchBatchFlushCount = 1
var ElasticsearchBatchFlushInterval = 10 * time.Second
var ElasticsearchIdentityField = `_id`
var ElasticsearchDocumentType = `_doc`
var ElasticsearchScrollLifetime = `1m`

var ElasticsearchRequestTimeout = 30 * time.Second
var ElasticsearchConnectTimeout = 3 * time.Second
var ElasticsearchTLSTimeout = 10 * time.Second
var ElasticsearchResponseHeaderTimeout = 10 * time.Second

type elasticsearchIndex struct {
	Name     string                 `json:"_index"`
	Mappings map[string]interface{} `json:"mappings"`
	Settings map[string]interface{} `json:"settings"`
}

type elasticsearchDocument struct {
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	ID      interface{}            `json:"_id"`
	Version int                    `json:"_version"`
	Score   float64                `json:"_score"`
	Found   bool                   `json:"found"`
	Source  map[string]interface{} `json:"_source"`
}

func (self *elasticsearchDocument) record(collection *dal.Collection, sep string) (*dal.Record, error) {
	if ids := sliceutil.Sliceify(self.Keys(sep)); len(ids) == collection.KeyCount() {
		var record = dal.NewRecord(nil).SetFields(self.Source)

		if err := record.SetKeys(collection, dal.RetrieveOperation, ids...); err != nil {
			return nil, err
		}

		if err := record.Populate(record, collection); err != nil {
			return nil, err
		}

		return record, nil
	} else {
		return nil, fmt.Errorf("%v: expected %d key values, got %d", self, collection.KeyCount(), len(ids))
	}
}

func (self *elasticsearchDocument) Keys(sep string) []string {
	var id = typeutil.String(self.ID)
	return strings.Split(id, sep)
}

type hits struct {
	Hits     []elasticsearchDocument `json:"hits"`
	MaxScore float64                 `json:"max_score"`
	Total    int64                   `json:"total"`
}

type elasticsearchSearchResult struct {
	Hits     hits   `json:"hits"`
	TimedOut bool   `json:"timed_out"`
	Took     int    `json:"took"`
	ScrollId string `json:"_scroll_id,omitempty"`
}

type elasticsearchScrollRequest struct {
	ScrollLifetime string `json:"scroll"`
	ScrollId       string `json:"scroll_id"`
}

type bulkOpType string

const (
	bulkIndex  bulkOpType = `index`
	bulkCreate            = `create`
	bulkDelete            = `delete`
	bulkUpdate            = `update`
)

type bulkOperation struct {
	Type    bulkOpType
	Index   string
	DocType string
	ID      interface{}
	Payload map[string]interface{}
}

func (self *bulkOperation) GetBody() ([]map[string]interface{}, error) {
	var rv []map[string]interface{}

	if self.Index == `` {
		return nil, fmt.Errorf("Index name is required for bulk operation")
	}

	if self.Type == `` {
		return nil, fmt.Errorf("Document Type is required for bulk operation")
	}

	if self.ID == `` {
		return nil, fmt.Errorf("ID is required for bulk operation")
	}

	// add the operation header, which is the same for all operation types
	rv = append(rv, map[string]interface{}{
		string(self.Type): map[string]interface{}{
			`_index`: self.Index,
			`_type`:  self.DocType,
			`_id`:    self.ID,
		},
	})

	// perform operation-specific validation and additions
	switch self.Type {
	case bulkIndex, bulkCreate, bulkUpdate:
		if len(self.Payload) == 0 {
			return nil, fmt.Errorf("Bulk %v operation requires a payload", self.Type)
		}

		var payload map[string]interface{}

		if self.Type == bulkUpdate {
			payload = map[string]interface{}{
				ElasticsearchDefaultType: self.Payload,
			}
		} else {
			payload = self.Payload
		}

		// make sure these aren't in the document itself, as they are metadata
		delete(payload, `_id`)
		delete(payload, `_index`)
		delete(payload, `_type`)

		rv = append(rv, payload)
	}

	return rv, nil
}

type esDeferredBatch struct {
	batch     []bulkOperation
	lastFlush time.Time
	batchLock sync.Mutex
}

func (self *esDeferredBatch) Add(op bulkOperation) {
	self.batchLock.Lock()
	defer self.batchLock.Unlock()
	self.batch = append(self.batch, op)
}

func (self *esDeferredBatch) Flush() ([]map[string]interface{}, error) {
	var rv []map[string]interface{}

	self.batchLock.Lock()

	defer func() {
		self.batch = nil
		self.lastFlush = time.Now()
		self.batchLock.Unlock()
	}()

	for _, op := range self.batch {
		if body, err := op.GetBody(); err == nil {
			rv = append(rv, body...)
		} else {
			return nil, err
		}
	}

	return rv, nil
}

type ElasticsearchIndexer struct {
	Indexer
	conn               *dal.ConnectionString
	parent             Backend
	indexCache         map[string]*elasticsearchIndex
	indexDeferredBatch *esDeferredBatch
	client             *httputil.Client
	refresh            string
	pkSeparator        string
}

func NewElasticsearchIndexer(connection dal.ConnectionString) *ElasticsearchIndexer {
	return &ElasticsearchIndexer{
		conn:               &connection,
		indexCache:         make(map[string]*elasticsearchIndex),
		indexDeferredBatch: new(esDeferredBatch),
		refresh:            ElasticsearchDefaultRefresh,
		pkSeparator:        ElasticsearchDefaultCompositeJoiner,
	}
}

func (self *ElasticsearchIndexer) IndexConnectionString() *dal.ConnectionString {
	return self.conn
}

func (self *ElasticsearchIndexer) IndexInitialize(parent Backend) error {
	if self.client == nil {
		if client, err := httputil.NewClient(
			fmt.Sprintf(
				"%s://%s",
				self.conn.Protocol(ElasticsearchDefaultScheme),
				self.conn.Host(ElasticsearchDefaultHost),
			),
		); err == nil {
			self.client = client

			self.client.SetErrorDecoder(esErrorDecoder)
			self.client.Client().Timeout = ElasticsearchRequestTimeout
			self.client.SetInsecureTLS(self.conn.OptBool(`insecure`, false))
			self.client.SetHeader(`Content-Type`, `application/json`)
			self.client.SetHeader(`Accept-Encoding`, `identity`)
			self.client.SetHeader(`User-Agent`, ClientUserAgent)

		} else {
			return err
		}
	}

	self.parent = parent

	return nil
}

func (self *ElasticsearchIndexer) GetBackend() Backend {
	return self.parent
}

func (self *ElasticsearchIndexer) IndexRetrieve(collection *dal.Collection, id interface{}) (*dal.Record, error) {
	defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.retrieve_time`)

	if index, err := self.getIndexForCollection(collection); err == nil {
		if response, err := self.client.Get(
			fmt.Sprintf(
				"/%s/%s/%v",
				index.Name,
				ElasticsearchDocumentType,
				id,
			),
			nil,
			nil,
		); err == nil {
			var doc elasticsearchDocument

			if err := json.NewDecoder(response.Body).Decode(&doc); err == nil {
				return doc.record(collection, self.pkSeparator)
			} else {
				return nil, fmt.Errorf("decode error: %v", err)
			}
		} else {
			return nil, fmt.Errorf("request error: %v", err)
		}
	} else {
		return nil, err
	}
}

func (self *ElasticsearchIndexer) IndexExists(collection *dal.Collection, id interface{}) bool {
	if _, err := self.IndexRetrieve(collection, id); err == nil {
		return true
	}

	return false
}

func (self *ElasticsearchIndexer) Index(collection *dal.Collection, records *dal.RecordSet) error {
	defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.index_time`)

	if index, err := self.getIndexForCollection(collection); err == nil {
		for _, record := range records.Records {
			querylog.Debugf("[%T] Adding %v to batch", self, record)

			self.indexDeferredBatch.Add(bulkOperation{
				Type:    bulkIndex,
				Index:   index.Name,
				DocType: ElasticsearchDocumentType,
				ID:      record.ID,
				Payload: record.Fields,
			})
		}

		self.checkAndFlushBatches(false)
		return nil
	} else {
		return err
	}
}

func (self *ElasticsearchIndexer) checkAndFlushBatches(forceFlush bool) {
	if l := len(self.indexDeferredBatch.batch); l > 0 {
		shouldFlush := false

		if l >= ElasticsearchBatchFlushCount {
			shouldFlush = true
		}

		if time.Since(self.indexDeferredBatch.lastFlush) >= ElasticsearchBatchFlushInterval {
			shouldFlush = true
		}

		if forceFlush {
			shouldFlush = true
		}

		if shouldFlush {
			defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.deferred_batch_flush`)

			if bulkBody, err := self.indexDeferredBatch.Flush(); err == nil {
				querylog.Debugf("[%T] Indexing %d records to %s", self, l)

				var lines []string

				for _, bb := range bulkBody {
					if b, err := json.Marshal(bb); err == nil {
						lines = append(lines, string(b))
					}
				}

				if _, err := self.client.Post(
					`/_bulk`,
					httputil.Literal(
						strings.Join(lines, "\n")+"\n",
					),
					map[string]interface{}{
						`refresh`: self.refresh,
					},
					nil,
				); err != nil {
					log.Errorf("[%T] error indexing %d records: %v", self, l, err)
				}
			} else {
				log.Errorf("[%T] error indexing %d records: %v", self, l, err)
			}
		}
	}
}

func (self *ElasticsearchIndexer) QueryFunc(collection *dal.Collection, f *filter.Filter, resultFn IndexResultFunc) error {
	defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.query_time`)

	if f.IdentityField == `` {
		if collection != nil {
			f.IdentityField = collection.GetIdentityFieldName()
		} else {
			f.IdentityField = ElasticsearchIdentityField
		}
	}

	// for i, crit := range f.Criteria {
	// 	switch crit.Field {
	// 	case ElasticsearchIdentityField, `id`:
	// 		f.Criteria[i].Field = sliceutil.OrString(
	// 			collection.IdentityField,
	// 			ElasticsearchIdentityField,
	// 		)
	// 	}
	// }

	if index, err := self.getIndexForCollection(collection); err == nil {
		var useScrollApi bool
		var lastScrollId string
		var processed int
		var originalLimit = f.Limit
		var originalOffset = f.Offset
		var isFirstScrollRequest = true
		var page = 1

		// shortpath for queries that specify both components of a composite key and nothing else
		if ckeys := self.compositeKeyId(collection, f, self.pkSeparator); ckeys != `` {
			if record, err := self.IndexRetrieve(collection, ckeys); err == nil {
				return resultFn(record, nil, IndexPage{
					Page:         1,
					TotalPages:   1,
					Limit:        originalLimit,
					Offset:       f.Offset,
					TotalResults: 1,
				})
			} else {
				return err
			}
		}

		// unbounded requests, or bounded ones exceeding 10k results, need to use the Scroll API
		// see: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html
		if f.Limit == 0 || f.Limit > 10000 {
			f.Limit = IndexerPageSize
			useScrollApi = true
		} else if f.Limit > IndexerPageSize {
			f.Limit = IndexerPageSize
		}

		defer func() {
			f.Offset = originalOffset
			f.Limit = originalLimit
		}()

		// perform requests until we have enough results or the index is out of them
		for {
			if query, err := filter.Render(
				generators.NewElasticsearchGenerator(),
				index.Name,
				f,
			); err == nil {
				var urlpath string
				var body interface{}

				// build the search request; either the initial Scroll API query, Scroll paging query,
				// or just a regular old Search API query.
				if useScrollApi && isFirstScrollRequest {
					isFirstScrollRequest = false
					urlpath = fmt.Sprintf("/%s/_search?scroll="+ElasticsearchScrollLifetime, index.Name)
					body = httputil.Literal(query)

				} else if useScrollApi {
					urlpath = `/_search/scroll`
					body = &elasticsearchScrollRequest{
						ScrollLifetime: ElasticsearchScrollLifetime,
						ScrollId:       lastScrollId,
					}
				} else {
					urlpath = fmt.Sprintf("/%s/_search", index.Name)
					body = httputil.Literal(query)
				}

				// perform request, read response
				if response, err := self.client.GetWithBody(urlpath, body, nil, nil); err == nil {
					var searchResult elasticsearchSearchResult

					if err := self.client.Decode(response.Body, &searchResult); err == nil {
						var results = searchResult.Hits
						lastScrollId = searchResult.ScrollId

						querylog.Debugf("[%T] Got %d/%d results", self, len(results.Hits), results.Total)

						if len(results.Hits) == 0 {
							return nil
						}

						// totalPages = ceil(result count / page size)
						var totalPages = int(math.Ceil(float64(results.Total) / float64(originalLimit)))

						if totalPages <= 0 {
							totalPages = 1
						}

						// call the resultFn for each hit on this page
						for _, hit := range results.Hits {
							if record, err := hit.record(collection, self.pkSeparator); err == nil {
								if err := resultFn(record, nil, IndexPage{
									Page:         page,
									TotalPages:   totalPages,
									Limit:        originalLimit,
									Offset:       f.Offset,
									TotalResults: int64(results.Total),
								}); err != nil {
									return err
								}

								processed += 1

								// if we have a limit set and we're at or beyond it
								if originalLimit > 0 && processed >= originalLimit {
									querylog.Debugf("[%T] %d at or beyond limit %d, returning results", self, processed, originalLimit)
									return nil
								}
							} else {
								return err
							}
						}

						// increment offset by the page size we just processed
						page += 1
						f.Offset += len(results.Hits)

						// if the offset is now beyond the total results count
						if int64(processed) >= results.Total {
							querylog.Debugf("[%T] %d at or beyond total %d, returning results", self, processed, results.Total)
							return nil
						}
					} else {
						return fmt.Errorf("%v: %s", response.StatusCode, err)
					}
				} else {
					return err
				}
			} else {
				return err
			}
		}
	} else {
		return err
	}
}

func (self *ElasticsearchIndexer) Query(collection *dal.Collection, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	var recordset = dal.NewRecordSet()

	if f.IdentityField == `` {
		f.IdentityField = ElasticsearchIdentityField
	}

	if err := self.QueryFunc(collection, f, func(record *dal.Record, err error, page IndexPage) error {
		if err == nil {
			recordset.Push(record)
			PopulateRecordSetPageDetails(recordset, f, page)
			return nil
		} else {
			return err
		}
	}); err == nil {
		return recordset, nil
	} else {
		return nil, err
	}
}

func (self *ElasticsearchIndexer) IndexRemove(collection *dal.Collection, ids []interface{}) error {
	if index, err := self.getIndexForCollection(collection); err == nil {
		var merr error

		for _, id := range ids {
			_, err := self.client.Delete(
				fmt.Sprintf("/%v/%v/%v", index.Name, ElasticsearchDocumentType, id),
				nil,
				nil,
			)

			log.AppendError(merr, err)
		}

		return merr
	} else {
		return err
	}
}

func (self *ElasticsearchIndexer) ListValues(collection *dal.Collection, fields []string, f *filter.Filter) (map[string][]interface{}, error) {
	if f == nil {
		f = filter.All()
	}

	if index, err := self.getIndexForCollection(collection); err == nil {
		var aggs = make(map[string]interface{})

		for _, field := range fields {
			aggs[field] = map[string]interface{}{
				`terms`: map[string]interface{}{
					`field`: field,
				},
			}
		}

		f.Options = map[string]interface{}{
			`aggs`: aggs,
		}

		if query, err := filter.Render(
			generators.NewElasticsearchGenerator(),
			index.Name,
			f,
		); err == nil {
			if response, err := self.client.GetWithBody(
				fmt.Sprintf("/%s/_search", index.Name),
				httputil.Literal(query),
				nil,
				nil,
			); err == nil {
				var aggResponse = make(map[string]interface{})

				if err := self.client.Decode(response.Body, &aggResponse); err == nil {
					var out = make(map[string][]interface{})

					for k, v := range maputil.M(aggResponse).Get(`aggregations`).MapNative() {
						var values = maputil.Pluck(
							maputil.M(v).Get(`buckets`).Value,
							[]string{`key`},
						)

						for i, v := range values {
							values[i] = collection.ConvertValue(k, v)
						}

						out[k] = values
					}

					return out, nil
				} else {
					return nil, fmt.Errorf("response decode error: %v", err)
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

func (self *ElasticsearchIndexer) DeleteQuery(collection *dal.Collection, f *filter.Filter) error {
	f.Fields = []string{ElasticsearchIdentityField}
	var ids []interface{}

	if err := self.QueryFunc(collection, f, func(indexRecord *dal.Record, err error, page IndexPage) error {
		ids = append(ids, indexRecord.ID)
		return nil
	}); err == nil {
		return self.parent.Delete(collection.Name, ids)
	} else {
		return err
	}
}

func (self *ElasticsearchIndexer) FlushIndex() error {
	self.checkAndFlushBatches(true)
	return nil
}

func (self *ElasticsearchIndexer) getIndexForCollection(collection *dal.Collection) (*elasticsearchIndex, error) {
	defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.retrieve_index`)
	var name = collection.GetIndexName()

	if v, ok := self.indexCache[name]; ok {
		return v, nil
	} else {
		if response, err := self.client.Get(
			fmt.Sprintf("/%s", name),
			map[string]interface{}{
				`include_type_name`: false,
			},
			nil,
		); err == nil {
			var index elasticsearchIndex

			if err := self.client.Decode(response.Body, &index); err == nil {
				index.Name = name
				self.indexCache[name] = &index

				return &index, nil
			} else {
				return nil, err
			}
		} else if response.StatusCode == 404 {
			return nil, fmt.Errorf("Index %v not found", name)
		} else {
			return nil, err
		}
	}
}

func (self *ElasticsearchIndexer) compositeKeyId(collection *dal.Collection, flt *filter.Filter, sep string) string {
	if flt != nil && len(flt.Criteria) == collection.KeyCount() {
		var parts []string

		for _, crit := range flt.Criteria {
			if collection.IsIdentityField(crit.Field) || crit.Field == ElasticsearchIdentityField {
				parts = append(parts, sliceutil.Stringify(crit.Values)...)
			} else if collection.IsKeyField(crit.Field) {
				parts = append(parts, sliceutil.Stringify(crit.Values)...)
			}
		}

		if len(parts) == len(flt.Criteria) {
			return strings.Join(parts, sep)
		}
	}

	return ``
}
