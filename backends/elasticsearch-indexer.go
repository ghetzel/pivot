package backends

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
)

var ElasticsearchBatchFlushCount = 1
var ElasticsearchBatchFlushInterval = 10 * time.Second
var ElasticsearchIdentityField = `_id`
var ElasticsearchDocumentType = `document`

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

type hits struct {
	Hits     []elasticsearchDocument `json:"hits"`
	MaxScore float64                 `json:"max_score"`
	Total    int64                   `json:"total"`
}

type elasticsearchSearchResult struct {
	Hits     hits `json:"hits"`
	TimedOut bool `json:"timed_out"`
	Took     int  `json:"took"`
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
			`_type`:  self.Type,
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
				`doc`: self.Payload,
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
	client             *http.Client
}

func NewElasticsearchIndexer(connection dal.ConnectionString) *ElasticsearchIndexer {
	return &ElasticsearchIndexer{
		conn:               &connection,
		indexCache:         make(map[string]*elasticsearchIndex),
		indexDeferredBatch: new(esDeferredBatch),
		client: &http.Client{
			Timeout: ElasticsearchRequestTimeout,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   ElasticsearchConnectTimeout,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout:   ElasticsearchTLSTimeout,
				ResponseHeaderTimeout: ElasticsearchResponseHeaderTimeout,
			},
		},
	}
}

func (self *ElasticsearchIndexer) IndexConnectionString() *dal.ConnectionString {
	return self.conn
}

func (self *ElasticsearchIndexer) IndexInitialize(parent Backend) error {
	self.parent = parent

	return nil
}

func (self *ElasticsearchIndexer) IndexRetrieve(name string, id interface{}) (*dal.Record, error) {
	defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.retrieve_time`)

	if index, err := self.getIndexForCollection(name); err == nil {
		if req, err := self.newRequest(`GET`, fmt.Sprintf("/%v/%v/%v", index.Name), nil); err == nil {
			if response, err := self.client.Do(req); err == nil {
				if response.StatusCode < 400 {
					var doc elasticsearchDocument

					if err := json.NewDecoder(response.Body).Decode(&doc); err == nil {
						return dal.NewRecord(doc.ID).SetFields(doc.Source), nil
					} else {
						return nil, fmt.Errorf("decode error: %v", err)
					}
				} else {
					return nil, fmt.Errorf("%v", response.Status)
				}
			} else {
				return nil, fmt.Errorf("response error: %v", err)
			}
		} else {
			return nil, fmt.Errorf("request error: %v", err)
		}
	} else {
		return nil, err
	}
}

func (self *ElasticsearchIndexer) IndexExists(collection string, id interface{}) bool {
	if _, err := self.IndexRetrieve(collection, id); err == nil {
		return true
	}

	return false
}

func (self *ElasticsearchIndexer) Index(name string, records *dal.RecordSet) error {
	defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.index_time`)

	if index, err := self.getIndexForCollection(name); err == nil {
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

				if req, err := self.newRequest(`POST`, `/_bulk`, bulkBody); err == nil {
					if response, err := self.client.Do(req); err == nil {
						if response.StatusCode >= 400 {
							log.Errorf("[%T] error indexing %d records: %v", self, l, response.Status)
						}
					} else {
						log.Errorf("[%T] error indexing %d records: %v", self, l, err)
					}
				} else {
					log.Errorf("[%T] error indexing %d records: %v", self, l, err)
				}
			} else {
				log.Errorf("[%T] error indexing %d records: %v", self, l, err)
			}
		}
	}
}

func (self *ElasticsearchIndexer) QueryFunc(name string, f *filter.Filter, resultFn IndexResultFunc) error {
	defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.query_time`)

	if f.IdentityField == `` {
		f.IdentityField = ElasticsearchIdentityField
	}

	if index, err := self.getIndexForCollection(name); err == nil {
		limit := f.Limit

		if limit == 0 || limit > IndexerPageSize {
			limit = IndexerPageSize
		}

		originalOffset := f.Offset

		defer func() {
			f.Offset = originalOffset
		}()

		page := 1
		processed := 0

		// perform requests until we have enough results or the index is out of them
		for {
			if query, err := filter.Render(
				generators.NewElasticsearchGenerator(),
				index.Name,
				f,
			); err == nil {
				if req, err := self.newRequest(`GET`, fmt.Sprintf("/%s/_search", index.Name), string(query)); err == nil {
					if response, err := self.client.Do(req); err == nil {
						if response.StatusCode < 400 {
							var searchResult elasticsearchSearchResult

							if err := json.NewDecoder(response.Body).Decode(&searchResult); err == nil {
								results := searchResult.Hits

								querylog.Debugf("[%T] Got %d/%d results", self, len(results.Hits), results.Total)

								if len(results.Hits) == 0 {
									return nil
								}

								// totalPages = ceil(result count / page size)
								totalPages := int(math.Ceil(float64(results.Total) / float64(f.Limit)))

								if totalPages <= 0 {
									totalPages = 1
								}

								// call the resultFn for each hit on this page
								for _, hit := range results.Hits {
									if err := resultFn(dal.NewRecord(hit.ID).SetFields(hit.Source), nil, IndexPage{
										Page:         page,
										TotalPages:   totalPages,
										Limit:        f.Limit,
										Offset:       f.Offset,
										TotalResults: int64(results.Total),
									}); err != nil {
										return err
									}

									processed += 1

									// if we have a limit set and we're at or beyond it
									if f.Limit > 0 && processed >= f.Limit {
										querylog.Debugf("[%T] %d at or beyond limit %d, returning results", self, processed, f.Limit)
										return nil
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
								return err
							}
						} else {
							var errbody map[string]interface{}
							json.NewDecoder(response.Body).Decode(&errbody)
							reason := strings.Join(
								sliceutil.Stringify(
									maputil.Pluck(maputil.DeepGet(errbody, []string{
										`error`, `root_cause`,
									}), []string{`reason`}),
								),
								`; `,
							)

							return fmt.Errorf("%v: %s", response.Status, sliceutil.Or(reason, `Unknown Error`))
						}
					} else {
						return err
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

func (self *ElasticsearchIndexer) Query(name string, f *filter.Filter, resultFns ...IndexResultFunc) (*dal.RecordSet, error) {
	recordset := dal.NewRecordSet()

	if f.IdentityField == `` {
		f.IdentityField = ElasticsearchIdentityField
	}

	if err := self.QueryFunc(name, f, func(indexRecord *dal.Record, err error, page IndexPage) error {
		PopulateRecordSetPageDetails(recordset, f, page)
		emptyRecord := dal.NewRecord(indexRecord.ID)

		if len(resultFns) > 0 {
			resultFn := resultFns[0]

			if f.IdOnly() {
				return resultFn(emptyRecord, err, page)
			} else {
				if record, err := self.parent.Retrieve(name, indexRecord.ID, f.Fields...); err == nil {
					return resultFn(record, err, page)
				} else {
					return resultFn(emptyRecord, err, page)
				}
			}
		} else {
			if f.IdOnly() {
				recordset.Records = append(recordset.Records, emptyRecord)
			} else {
				if record, err := self.parent.Retrieve(name, indexRecord.ID, f.Fields...); err == nil {
					recordset.Records = append(recordset.Records, record)
				}
			}

			return nil
		}
	}); err != nil {
		return nil, err
	}

	return recordset, nil
}

func (self *ElasticsearchIndexer) IndexRemove(name string, ids []interface{}) error {
	if index, err := self.getIndexForCollection(name); err == nil {
		for _, id := range ids {
			if req, err := self.newRequest(
				`DELETE`,
				fmt.Sprintf("/%v/%v/%v", index.Name, ElasticsearchDocumentType, id),
				nil,
			); err == nil {
				self.client.Do(req)
			}
		}

		return nil
	} else {
		return err
	}
}

func (self *ElasticsearchIndexer) ListValues(collection string, fields []string, f *filter.Filter) (map[string][]interface{}, error) {
	if _, err := self.getIndexForCollection(collection); err == nil {
		return nil, fmt.Errorf("Not Implemented")
	} else {
		return nil, err
	}
}

func (self *ElasticsearchIndexer) DeleteQuery(name string, f *filter.Filter) error {
	f.Fields = []string{ElasticsearchIdentityField}
	var ids []interface{}

	if err := self.QueryFunc(name, f, func(indexRecord *dal.Record, err error, page IndexPage) error {
		ids = append(ids, indexRecord.ID)
		return nil
	}); err == nil {
		return self.parent.Delete(name, ids)
	} else {
		return err
	}
}

func (self *ElasticsearchIndexer) FlushIndex() error {
	self.checkAndFlushBatches(true)
	return nil
}

func (self *ElasticsearchIndexer) newRequest(method string, urlpath string, body interface{}) (*http.Request, error) {
	var buf bytes.Buffer
	var lines []string

	for _, item := range sliceutil.Sliceify(body) {
		if str, ok := item.(string); ok {
			// strings go through as-is
			lines = append(lines, str)

		} else if data, err := json.Marshal(item); err == nil {
			// everything else gets jsonified
			lines = append(lines, string(data))
		} else {
			return nil, err
		}
	}

	buf.Write([]byte(strings.Join(lines, "\n")))

	querylog.Debugf("[%T] %v %v", self, method, urlpath)

	host := self.conn.Host()
	protocol := sliceutil.Or(self.conn.Protocol(), `http`)

	if req, err := http.NewRequest(
		method,
		fmt.Sprintf("%s://%s/%s", protocol, host, strings.Trim(urlpath, `/`)),
		&buf,
	); err == nil {
		req.Header.Set(`Content-Type`, `application/json`)

		return req, nil
	} else {
		return nil, err
	}
}

func (self *ElasticsearchIndexer) getIndexForCollection(name string) (*elasticsearchIndex, error) {
	defer stats.NewTiming().Send(`pivot.indexers.elasticsearch.retrieve_index`)

	if v, ok := self.indexCache[name]; ok {
		return v, nil
	} else {
		if req, err := self.newRequest(`GET`, fmt.Sprintf("/%s", name), nil); err == nil {
			if response, err := self.client.Do(req); err == nil {
				switch {
				case response.StatusCode < 400:
					var index elasticsearchIndex

					if err := json.NewDecoder(response.Body).Decode(&index); err == nil {
						index.Name = name
						self.indexCache[name] = &index

						return &index, nil
					} else {
						return nil, err
					}

				case response.StatusCode == 404:
					return nil, fmt.Errorf("Index %v not found", name)

				default:
					return nil, err
				}
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
}

func (self *ElasticsearchIndexer) useFilterMapping(index *elasticsearchIndex) {
	// mappingImpl.AddCustomCharFilter(`remove_expression_tokens`, map[string]interface{}{
	// 	`type`:   regexp.Name,
	// 	`regexp`: `[\:\[\]\*]+`,
	// })

	// mappingImpl.AddCustomAnalyzer(`pivot_filter`, map[string]interface{}{
	// 	`type`: custom.Name,
	// 	`char_filters`: []string{
	// 		`remove_expression_tokens`,
	// 	},
	// 	`tokenizer`: single.Name,
	// 	`token_filters`: []string{
	// 		lowercase.Name,
	// 	},
	// })

	// mappingImpl.DefaultAnalyzer = `pivot_filter`
}
