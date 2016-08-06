package elasticsearch

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/op/go-logging"
	"strings"
	"time"
)

var log = logging.MustGetLogger(`backends`)

type ElasticsearchBackend struct {
	backends.Backend
	Connected  bool `json:"connected"`
	client     *ElasticsearchClient
	maxRetries int
	esVersion  int
}

type ElasticsearchQuery struct {
	filter filter.Filter
}

const (
	DEFAULT_ES_MAPPING_REFRESH = (30 * time.Second)
	DEFAULT_ES_HOST            = `http://localhost:9200`
	DEFAULT_ES_DOCUMENT_TYPE   = `document`
)

func New(name string, config dal.Dataset) *ElasticsearchBackend {
	config.Collections = make([]dal.Collection, 0)

	// specify that field order does not matter to Elasticsearch
	config.FieldsUnordered = true

	// specify that Elasticsearch field lengths should not be compared
	config.SkipFieldLength = true

	return &ElasticsearchBackend{
		Backend: backends.Backend{
			Name:          name,
			Dataset:       config,
			SchemaRefresh: DEFAULT_ES_MAPPING_REFRESH,
		},
		client: NewClient(),
	}
}

func (self *ElasticsearchBackend) SetConnected(c bool) {
	self.Connected = c

	if self.Connected {
		self.client.Resume()
		log.Infof("CONNECT: backend %s", self.GetName())
	} else {
		self.client.Suspend()
		log.Warningf("DISCONNECT: backend %s", self.GetName())
	}
}

func (self *ElasticsearchBackend) IsConnected() bool {
	return self.Connected
}

func (self *ElasticsearchBackend) Disconnect() {
	self.SetConnected(false)
}

func (self *ElasticsearchBackend) Connect() error {
	if len(self.Dataset.Addresses) == 0 {
		self.Dataset.Addresses = []string{DEFAULT_ES_HOST}
	}

	if v, ok := self.Dataset.Options[`max_retries`]; ok {
		if value, err := stringutil.ConvertToInteger(v); err == nil {
			self.maxRetries = int(value)
		}
	}

	log.Debugf("Connecting to %s", strings.Join(self.Dataset.Addresses, `, `))

	self.client.SetAddresses(self.Dataset.Addresses...)

	if err := self.client.CheckQuorum(); err == nil {
		self.SetConnected(true)
	} else {
		return err
	}

	return self.Finalize(self)
}

func (self *ElasticsearchBackend) Refresh() error {
	if version, err := self.client.ServerVersion(); err == nil {
		self.esVersion = version
	} else {
		return err
	}

	if clusterHealth, err := self.client.ClusterHealth(); err == nil {
		self.Dataset.Name = clusterHealth.Name
		self.Dataset.Metadata = clusterHealth.ToMap()

		// if clusterHealth.Status == `red` {
		// 	return fmt.Errorf("Cannot read Elasticsearch cluster metadata: cluster '%s' has a status of 'red'", clusterHealth.Name)
		// }
	} else {
		return err
	}

	if indexNames, err := self.client.IndexNames(); err == nil {
		collections := make([]dal.Collection, 0)

		for _, index := range indexNames {
			if indexMappings, err := self.client.GetMapping(index); err == nil {
				fields := make([]dal.Field, 0)
				fieldMap := make(map[string]dal.Field)

				// for all mappings across all document types on this index...
				for docType, mapping := range indexMappings.Mappings {
					// for all top-level properties in a mapping...
					for fieldName, fieldConfigI := range mapping.Properties {
						switch fieldConfigI.(type) {
						case map[string]interface{}:
							fieldConfig := fieldConfigI.(map[string]interface{})
							fieldConfig[`document_type`] = docType

							var fieldType string

							if fieldTypeI, ok := fieldConfig[`type`]; ok {
								if f, err := stringutil.ToString(fieldTypeI); err == nil {
									delete(fieldConfig, `type`)

									if pivotType, err := self.client.ToPivotType(f); err == nil {
										fieldType = pivotType
									}
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

							fieldMap[fieldName] = field
						}
					}
				}

				for _, fieldName := range maputil.StringKeys(fieldMap) {
					if field, ok := fieldMap[fieldName]; ok {
						fields = append(fields, field)
					}
				}

				collections = append(collections, dal.Collection{
					Dataset:    &self.Dataset,
					Name:       index,
					Fields:     fields,
					Properties: map[string]interface{}{},
				})
			} else {
				return err
			}
		}

		self.Dataset.Collections = collections

	} else {
		return err
	}

	return nil
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

func (self *ElasticsearchBackend) UpdateCollectionSchema(action dal.CollectionAction, definition dal.Collection) error {
	defer self.Refresh()

	switch action {
	// CREATE ----------------------------------------------------------------------------------------------------------
	case dal.SchemaCreate:
		if existingDefinition, ok := self.Dataset.GetCollection(definition.Name); !ok {
			return self.client.CreateIndex(definition.Name, definition)
		} else {
			return existingDefinition.VerifyEqual(definition)
		}

	// VERIFY ----------------------------------------------------------------------------------------------------------
	case dal.SchemaVerify:
		if err := self.Refresh(); err == nil {
			if existingDefinition, ok := self.Dataset.GetCollection(definition.Name); ok {
				if err := existingDefinition.VerifyEqual(definition); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("Collection '%s' does not exist", definition.Name)
			}
		} else {
			return err
		}

	// EXPAND ----------------------------------------------------------------------------------------------------------
	case dal.SchemaExpand:

	// REMOVE ----------------------------------------------------------------------------------------------------------
	case dal.SchemaRemove:

	// ENFORCE ---------------------------------------------------------------------------------------------------------
	case dal.SchemaEnforce:

	}

	return fmt.Errorf("Not implemented")
}

func (self *ElasticsearchBackend) DeleteCollectionSchema(collectionName string) error {
	defer self.Refresh()

	if acknowledged, err := self.client.DeleteIndex(collectionName); err == nil {
		if acknowledged {
			return nil
		} else {
			return fmt.Errorf("Deletion of index '%s' was not acknowledged. The delete may not have been successful on all nodes.", collectionName)
		}
	} else {
		return err
	}
}

func (self *ElasticsearchBackend) GetRecords(collectionName string, f filter.Filter) (*dal.RecordSet, error) {
	if response, err := self.client.Search(collectionName, `document`, f); err == nil {
		rs := dal.NewRecordSet()

		if hits := response.Hits; hits.Total > 0 {
			rs.ResultCount = uint64(hits.Total)

			for _, hit := range hits.Hits {
				record := make(dal.Record)
				record[`_id`] = hit.ID

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
				for k, v := range hit.Source {
					if sliceutil.ContainsString(f.Fields, k) || len(f.Fields) == 0 {
						record[k] = v
					}
				}

				rs.Records = append(rs.Records, record)
			}
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
	docType := DEFAULT_ES_DOCUMENT_TYPE

	if v, ok := f.Options[`document_type`]; ok {
		docType = v
	}

	return self.client.DeleteByQuery(collectionName, docType, f)
}

func (self *ElasticsearchBackend) upsertDocument(collectionName string, f filter.Filter, payload *dal.RecordSet) error {
	if payload != nil {
		docType := DEFAULT_ES_DOCUMENT_TYPE

		if v, ok := payload.Options[`document_type`]; ok {
			switch v.(type) {
			case string:
				docType = v.(string)
			}
		}

		switch len(payload.Records) {
		case 0:
			return fmt.Errorf("No records supplied in request")

		case 1:
			var id string

			for _, criterion := range f.Criteria {
				if criterion.Field == `id` {
					if len(criterion.Values) > 0 {
						id = criterion.Values[0]
						break
					}
				}
			}

			//  one of the few, few times we don't need an error check because we already
			//  ensured record 0 will exist with the length check in this switch :)
			record, _ := payload.GetRecord(0)

			//  _id wasn't in the Filter criteria, check the payload itself
			if id == `` {
				if v, ok := record[`id`]; ok {
					if s, err := stringutil.ToString(v); err == nil {
						id = s
					}
				}
			}

			if id == `` {
				return fmt.Errorf("Cannot insert/update document without an 'id' field in the query")
			} else {
				_, err := self.client.IndexDocument(collectionName, docType, id, record.ToMap())
				return err
			}

		default:
			// TODO: implement Bulk API
			return fmt.Errorf("Bulk insert not supported for this backend")
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
