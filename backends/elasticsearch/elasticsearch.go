package elasticsearch

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
	"github.com/ghetzel/pivot/patterns"
	"github.com/op/go-logging"
	"gopkg.in/olivere/elastic.v2"
	"strings"
	"time"
)

var log = logging.MustGetLogger(`backends`)

type ElasticsearchBackend struct {
	backends.Backend
	patterns.IRecordAccessPattern
	Connected bool
	client    *elastic.Client
	querygen  *generators.Elasticsearch
}

type ElasticsearchQuery struct {
	elastic.Query
	filter filter.Filter
}

func NewElasticsearchQuery(f filter.Filter) *ElasticsearchQuery {
	return &ElasticsearchQuery{
		filter: f,
	}
}

func (self *ElasticsearchQuery) Source() interface{} {
	generator := generators.NewElasticsearchGenerator()

	if data, err := filter.Render(generator, ``, self.filter); err == nil {
		var rv map[string]interface{}

		if err := json.Unmarshal(data, &rv); err == nil {
			//  Elasticsearch 1.x: "filtered" query
			//    (deprecated in 2.0.0-beta1, see: https://www.elastic.co/guide/en/elasticsearch/reference/2.0/query-dsl-filtered-query.html)
			return map[string]interface{}{
				`filtered`: map[string]interface{}{
					`filter`: rv,
				},
			}
		}
	}

	return nil
}

const (
	DEFAULT_ES_MAPPING_REFRESH = (30 * time.Second)
	DEFAULT_ES_HOST            = `http://localhost:9200`
	DEFAULT_ES_DOCUMENT_TYPE   = `document`
)

func New(name string, config dal.Dataset) *ElasticsearchBackend {
	// config.Pattern      = pattern
	config.Collections = make([]dal.Collection, 0)

	return &ElasticsearchBackend{
		Backend: backends.Backend{
			Name:          name,
			Dataset:       config,
			SchemaRefresh: DEFAULT_ES_MAPPING_REFRESH,
		},
		querygen: generators.NewElasticsearchGenerator(),
	}
}

func (self *ElasticsearchBackend) SetConnected(c bool) {
	self.Connected = c

	if c {
		log.Infof("CONNECT: backend %s", self.GetName())
	} else {
		log.Warningf("DISCONNECT: backend %s", self.GetName())
	}
}

func (self *ElasticsearchBackend) IsConnected() bool {
	return self.Connected
}

func (self *ElasticsearchBackend) Disconnect() {
	if self.client != nil {
		defer self.client.Stop()
	}

	self.SetConnected(false)
}

func (self *ElasticsearchBackend) Connect() error {
	if len(self.Dataset.Addresses) == 0 {
		self.Dataset.Addresses = []string{DEFAULT_ES_HOST}
	}

	clientConfig := []elastic.ClientOptionFunc{
		elastic.SetURL(self.Dataset.Addresses...),
	}

	if v, ok := self.Dataset.Options[`max_retries`]; ok {
		if value, err := stringutil.ConvertToInteger(v); err == nil {
			clientConfig = append(clientConfig, elastic.SetMaxRetries(int(value)))
		}
	}

	if v, ok := self.Dataset.Options[`healthcheck_interval`]; ok {
		if value, err := stringutil.ConvertToInteger(v); err == nil {
			if value == 0 {
				clientConfig = append(clientConfig, elastic.SetHealthcheck(false))
			} else {
				clientConfig = append(clientConfig, elastic.SetHealthcheck(true))
				clientConfig = append(clientConfig, elastic.SetHealthcheckInterval(time.Second*time.Duration(value)))
			}
		}
	} else {
		clientConfig = append(clientConfig, elastic.SetHealthcheck(true))
		clientConfig = append(clientConfig, elastic.SetHealthcheckInterval(self.RefreshInterval()))
	}

	log.Debugf("Connecting to %s", strings.Join(self.Dataset.Addresses, `, `))

	if client, err := elastic.NewClient(clientConfig...); err == nil {
		self.client = client
		self.SetConnected(true)
	} else {
		return err
	}

	return self.Finalize(self)
}

func (self *ElasticsearchBackend) Refresh() error {
	// log.Debugf("Reloading schema cache for backend '%s' (type: %s)", self.GetName(), self.Dataset.Type)
	if clusterHealth, err := self.client.ClusterHealth().Do(); err == nil {
		self.Dataset.Name = clusterHealth.ClusterName

		self.Dataset.Metadata = make(map[string]interface{})

		self.Dataset.Metadata[`status`] = clusterHealth.Status
		self.Dataset.Metadata[`number_of_nodes`] = clusterHealth.NumberOfNodes
		self.Dataset.Metadata[`number_of_data_nodes`] = clusterHealth.NumberOfDataNodes
		self.Dataset.Metadata[`active_primary_shards`] = clusterHealth.ActivePrimaryShards
		self.Dataset.Metadata[`active_shards`] = clusterHealth.ActiveShards
		self.Dataset.Metadata[`initializing_shards`] = clusterHealth.InitializingShards
		self.Dataset.Metadata[`unassigned_shards`] = clusterHealth.UnassignedShards
		self.Dataset.Metadata[`number_of_pending_tasks`] = clusterHealth.NumberOfPendingTasks

		if clusterHealth.Status == `red` {
			return fmt.Errorf("Cannot read Elasticsearch cluster metadata: cluster '%s' has a status of 'red'", clusterHealth.ClusterName)
		}
	} else {
		return err
	}

	if indexNames, err := self.client.IndexNames(); err == nil {
		collections := make([]dal.Collection, 0)

		for _, index := range indexNames {
			getMapping := self.client.GetMapping()

			getMapping.Index(index)

			fields := make([]dal.Field, 0)

			if mapping, err := getMapping.Do(); err == nil {
				if typeTopLevelMapI := maputil.DeepGet(mapping, []string{index, `mappings`}, nil); typeTopLevelMapI != nil {
					switch typeTopLevelMapI.(type) {
					case map[string]interface{}:
						for docType, propertiesI := range typeTopLevelMapI.(map[string]interface{}) {
							switch propertiesI.(type) {
							case map[string]interface{}:
								if fieldTopLevelI := maputil.DeepGet(propertiesI.(map[string]interface{}), []string{`properties`}, nil); fieldTopLevelI != nil {
									switch fieldTopLevelI.(type) {
									case map[string]interface{}:
										for fieldName, fieldConfigI := range fieldTopLevelI.(map[string]interface{}) {
											switch fieldConfigI.(type) {
											case map[string]interface{}:
												var fieldType string

												fieldConfig := fieldConfigI.(map[string]interface{})
												fieldConfig[`document_type`] = docType

												if fieldTypeI, ok := fieldConfig[`type`]; ok {
													var err error
													if fieldType, err = stringutil.ToString(fieldTypeI); err == nil {
														delete(fieldConfig, `type`)
													}
												}

												if fieldType == `` {
													fieldType = `object`
												}

												delete(fieldConfig, `properties`)

												field := dal.Field{
													Name:       fieldName,
													Type:       fieldType,
													Properties: fieldConfig,
												}

												fields = append(fields, field)
											}
										}
									}
								}
							}
						}
					}
				}
			}

			collections = append(collections, dal.Collection{
				Dataset:    &self.Dataset,
				Name:       index,
				Fields:     fields,
				Properties: map[string]interface{}{},
			})
		}

		self.Dataset.Collections = collections

	} else {
		return err
	}

	return nil
}

func (self *ElasticsearchBackend) Info() map[string]interface{} {
	return map[string]interface{}{}
}

func (self *ElasticsearchBackend) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		`type`:      `elasticsearch`,
		`connected`: self.IsConnected(),
		`available`: self.IsAvailable(),
	}
}

func (self *ElasticsearchBackend) ReadDatasetSchema() *dal.Dataset {
	return self.GetDataset()
}

func (self *ElasticsearchBackend) ReadCollectionSchema(collectionName string) (dal.Collection, bool) {
	for _, collection := range self.Dataset.Collections {
		if collection.Name == collectionName {
			return collection, true
		}
	}

	return dal.Collection{}, false
}

func (self *ElasticsearchBackend) UpdateCollectionSchema(action dal.CollectionAction, collectionName string, definition dal.Collection) error {
	switch action {
	//  CREATE ----------------------------------------------------------------------------------------------------------
	case dal.SchemaCreate:

		//  VERIFY ----------------------------------------------------------------------------------------------------------
	case dal.SchemaVerify:

		//  EXPAND ----------------------------------------------------------------------------------------------------------
	case dal.SchemaExpand:

		//  REMOVE ----------------------------------------------------------------------------------------------------------
	case dal.SchemaRemove:

		//  ENFORCE ---------------------------------------------------------------------------------------------------------
	case dal.SchemaEnforce:

	}

	return fmt.Errorf("Not implemented")
}

func (self *ElasticsearchBackend) DeleteCollectionSchema(collectionName string) error {
	if deleteIndex, err := self.client.DeleteIndex(collectionName).Do(); err == nil {
		if deleteIndex.Acknowledged {
			return nil
		} else {
			return fmt.Errorf("Deletion of index '%s' was not acknowledged. The delete may not have been successful on all nodes.", collectionName)
		}
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) GetRecords(collectionName string, f filter.Filter) (*dal.RecordSet, error) {
	query := NewElasticsearchQuery(f)

	search := self.client.Search()
	search.Index(collectionName)
	search.FetchSource(false)

	//  _source processing only happens if we want all fields, or are only looking
	//  at metadata fields
	if len(f.Fields) == 0 {
		search.FetchSource(true)
	} else {
		for _, field := range f.Fields {
			if !strings.HasPrefix(field, `_`) {
				search.FetchSource(true)
				break
			}
		}
	}

	//  we want the _version field, set the flag
	if sliceutil.ContainsString(f.Fields, `_version`) {
		search.Version(true)
	}

	//  limit result size
	if v, ok := f.Options[`page_size`]; ok {
		if i, err := stringutil.ConvertToInteger(v); err == nil {
			search.Size(int(i))
		} else {
			return nil, fmt.Errorf("Invalid 'page_size' parameter: %v", err)
		}
	}

	//  offset results
	if v, ok := f.Options[`offset`]; ok {
		if i, err := stringutil.ConvertToInteger(v); err == nil {
			search.From(int(i))
		} else {
			return nil, fmt.Errorf("Invalid 'offset' parameter: %v", err)
		}
	}

	if results, err := search.Query(query).Do(); err == nil {
		rs := dal.NewRecordSet()

		if hits := results.Hits; hits != nil {
			rs.ResultCount = uint64(hits.TotalHits)

			for _, hit := range hits.Hits {
				record := make(dal.Record)
				record[`_id`] = hit.Id

				//  tack on requested metadata as _-fields in result record
				for _, field := range f.Fields {
					switch field {
					case `_score`:
						record[`_score`] = hit.Score
					case `_type`:
						record[`_type`] = hit.Type
					case `_version`:
						record[`_version`] = hit.Version
					case `_index`:
						record[`_index`] = hit.Index
					}
				}

				//  only process _source if it is available
				if source := hit.Source; source != nil {
					if err := json.Unmarshal(*source, &record); err == nil {

						if len(f.Fields) > 0 {
							for k, _ := range record {
								if !sliceutil.ContainsString(f.Fields, k) {
									delete(record, k)
								}
							}
						}

					}
				}

				rs.Records = append(rs.Records, record)
			}
		} else {
			return nil, fmt.Errorf("Failed to read results")
		}

		return rs, nil
	} else {
		return nil, err
	}
}

func (self *ElasticsearchBackend) InsertRecords(collectionName string, f filter.Filter, payload *dal.RecordSet) error {
	return self.upsertDocument(collectionName, f, payload)
}

func (self *ElasticsearchBackend) UpdateRecords(collectionName string, f filter.Filter, payload *dal.RecordSet) error {
	return self.upsertDocument(collectionName, f, payload)
}

func (self *ElasticsearchBackend) DeleteRecords(collectionName string, f filter.Filter) error {
	query := NewElasticsearchQuery(f)
	dbq := self.client.DeleteByQuery()

	dbq.Index(collectionName)
	dbq.AllowNoIndices(true)

	if _, err := dbq.Query(query).Do(); err != nil {
		return err
	}

	return nil
}

func (self *ElasticsearchBackend) upsertDocument(collectionName string, f filter.Filter, payload *dal.RecordSet) error {
	if payload != nil {
		switch len(payload.Records) {
		case 0:
			return fmt.Errorf("No records supplied in request")

		case 1:
			var id string

			for _, criterion := range f.Criteria {
				if criterion.Field == `_id` {
					if len(criterion.Values) > 0 {
						id = criterion.Values[0]
						break
					}
				}
			}

			//  one of the few, few times we don't need an error check because we already
			//  ensured record 0 will exist with the length check in this switch :)
			record, _ := payload.GetRecord(0)

			//  _id wasn't in the Urlquery criteria, check the payload itself
			if id == `` {
				if v, ok := record[`_id`]; ok {
					if s, err := stringutil.ToString(v); err == nil {
						id = s
					}
				}
			}

			if id == `` {
				return fmt.Errorf("Cannot insert/update document without an '_id' field in the query")
			} else {

				updater := self.client.Update()
				updater.Index(collectionName)
				updater.Id(id)
				updater.DocAsUpsert(true)

				if typ, ok := f.Options[`document_type`]; ok {
					updater.Type(typ)
				} else {
					updater.Type(DEFAULT_ES_DOCUMENT_TYPE)
				}

				updater.Doc(record)

				if _, err := updater.Do(); err != nil {
					return err
				}
			}

		default:
			bulker := elastic.NewBulkService(self.client)

			for i, record := range payload.Records {
				if idData, ok := record[`_id`]; ok {
					if id, err := stringutil.ToString(idData); err == nil {
						bulkIndex := elastic.NewBulkIndexRequest()

						bulkIndex.Index(collectionName)
						bulkIndex.Id(id)

						if typ, ok := f.Options[`document_type`]; ok {
							bulkIndex.Type(typ)
						} else {
							bulkIndex.Type(DEFAULT_ES_DOCUMENT_TYPE)
						}

						bulkIndex.Doc(record)

						bulker.Add(bulkIndex)
					} else {
						return fmt.Errorf("Invalid record at index %d: _id value cannot be converted into a string", i)
					}
				} else {
					return fmt.Errorf("Invalid record at index %d: record is missing an '_id' field", i)
				}
			}

			if _, err := bulker.Do(); err != nil {
				return fmt.Errorf("Failed to execute batch statement: %v", err)
			}
		}
	} else {
		return fmt.Errorf("No recordset specified")
	}

	return nil
}

// func (self *ElasticsearchBackend) requestToFilter(req *http.Request, params httprouter.Params) (filter.Filter, error) {
// 	if f, err := filter.Parse(params.ByName(`query`)); err == nil {
// 		//  get fields from query string
// 		if fields := req.URL.Query().Get(`fields`); fields != `` {
// 			f.Fields = strings.Split(fields, `,`)
// 		}

// 		//  limit size of resultset
// 		if v := req.URL.Query().Get(`page_size`); v != `` {
// 			if _, err := strconv.ParseUint(v, 10, 32); err == nil {
// 				f.Options[`page_size`] = v
// 			}
// 		}

// 		//  limit size of resultset
// 		if v := req.URL.Query().Get(`offset`); v != `` {
// 			if _, err := strconv.ParseUint(v, 10, 32); err == nil {
// 				f.Options[`offset`] = v
// 			}
// 		}

// 		//  multifield will include an additional subfield in generated queries
// 		if v := req.URL.Query().Get(`multifield`); v != `` {
// 			f.Options[`multifield`] = v
// 		}

// 		// //  get the consistency level (if specified)
// 		//     if consistency := req.URL.Query().Get(`consistency`); consistency != `` {
// 		//         f.Options[`consistency`] = consistency
// 		//     }

// 		// //  set the size of pages to retrieve from cassandra
// 		//     if pageSize := req.URL.Query().Get(`page_size`); pageSize != `` {
// 		//         if _, err := strconv.ParseUint(pageSize, 10, 32); err == nil {
// 		//             f.Options[`page_size`] = pageSize
// 		//         }
// 		//     }

// 		// if pageState := req.URL.Query().Get(`pagestate`); page != `` {
// 		// }

// 		return f, nil
// 	} else {
// 		return filter.Filter{}, fmt.Errorf("Invalid query: %v", err)
// 	}
// }
