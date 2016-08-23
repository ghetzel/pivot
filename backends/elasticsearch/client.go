package elasticsearch

import (
	"fmt"
	"github.com/ghetzel/bee-hotel"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"math"
	"strings"
)

type ElasticsearchClient struct {
	*bee.MultiClient
}

func NewClient() *ElasticsearchClient {
	client := bee.NewMultiClient()

	return &ElasticsearchClient{
		MultiClient: client,
	}
}

func (self *ElasticsearchClient) ServerVersion() (int, error) {
	status := ServerStatus{}
	var version int

	if err := self.Request(`GET`, ``, nil, &status, nil); err == nil {
		if status.Version.Number != `` {
			parts := strings.Split(status.Version.Number, `.`)

			for i, numStr := range parts {
				if number, err := stringutil.ConvertToInteger(numStr); err == nil {
					version += int(number) * int(math.Pow(100, float64(len(parts)-i)))
				} else {
					return 1, err
				}
			}

			return version, nil
		} else {
			return -1, fmt.Errorf("Unable to retrieve version number from Elasticsearch")
		}
	} else {
		return -1, err
	}
}

func (self *ElasticsearchClient) ClusterHealth() (ClusterHealth, error) {
	health := ClusterHealth{}

	if err := self.Request(`GET`, `_cluster/health`, nil, &health, nil); err == nil {
		return health, nil
	} else {
		return health, err
	}
}

func (self *ElasticsearchClient) IndexNames() ([]string, error) {
	indexStats := IndexStats{}

	if err := self.Request(`GET`, `_stats/indices`, nil, &indexStats, nil); err == nil {
		return maputil.StringKeys(indexStats.Indices), nil
	} else {
		return []string{}, err
	}
}

func (self *ElasticsearchClient) GetMapping(index string) (IndexMapping, error) {
	mappingAllTypes := make(map[string]interface{})
	mapping := IndexMapping{
		IndexName: index,
		Mappings:  make(map[string]Mapping),
	}

	if err := self.Request(`GET`, fmt.Sprintf("%s/_mapping", index), nil, &mappingAllTypes, nil); err == nil {
		perTypeMappingsI := maputil.DeepGet(mappingAllTypes, []string{index, `mappings`}, nil)

		switch perTypeMappingsI.(type) {
		case map[string]interface{}:
			perTypeMappings := perTypeMappingsI.(map[string]interface{})
			documentTypes := maputil.StringKeys(perTypeMappings)

			for _, documentType := range documentTypes {
				if docMappingI, ok := perTypeMappings[documentType]; ok {
					switch docMappingI.(type) {
					case map[string]interface{}:
						docMapping := docMappingI.(map[string]interface{})

						if propertiesI, ok := docMapping[`properties`]; ok {
							switch propertiesI.(type) {
							case map[string]interface{}:
								properties := propertiesI.(map[string]interface{})

								mapping.Mappings[documentType] = Mapping{
									Type:       documentType,
									Properties: properties,
								}
							}
						}
					}
				}
			}
		}

		return mapping, nil

	} else {
		return mapping, err
	}
}

func (self *ElasticsearchClient) DeleteIndex(index string) (bool, error) {
	ackResponse := AckResponse{}

	if err := self.Request(`DELETE`, index, nil, &ackResponse, nil); err == nil {
		return ackResponse.Acknowledged, nil
	} else {
		return false, err
	}
}

func (self *ElasticsearchClient) Search(index string, docType string, f filter.Filter) (SearchResponse, error) {
	response := SearchResponse{}
	errResponse := ErrorResponse{}

	if searchRequest, err := NewSearchRequestFromFilter(index, docType, f); err == nil {
		requestBody := map[string]interface{}{
			`query`: searchRequest.Query,
		}

		for k, v := range searchRequest.Options {
			requestBody[k] = v
		}

		if err := self.Request(`GET`, fmt.Sprintf("%s/_search", index), &requestBody, &response, &errResponse); err == nil {
			return response, nil
		} else {
			if detailedError := errResponse.Error(); detailedError != nil {
				return response, detailedError
			} else {
				return response, err
			}
		}
	} else {
		return response, err
	}
}

func (self *ElasticsearchClient) DeleteByQuery(index string, docType string, f filter.Filter) error {
	ackResponse := AckResponse{}
	errResponse := ErrorResponse{}
	url := ``
	var payload interface{}

	// single-criterion filters that only specify _id are a special case and can use a more direct API
	if len(f.Criteria) == 1 && f.Criteria[0].Field == `_id` && len(f.Criteria[0].Values) == 1 {
		url = fmt.Sprintf("%s/%s/%s", index, docType, f.Criteria[0].Values[0])
		payload = nil
	} else {
		if searchRequest, err := NewSearchRequestFromFilter(index, docType, f); err == nil {
			url = fmt.Sprintf("%s/%s/_query", index, docType)
			payload = map[string]interface{}{
				`query`: searchRequest.Query,
			}
		} else {
			return err
		}
	}

	if err := self.Request(`DELETE`, url, payload, &ackResponse, &errResponse); err == nil {
		if ackResponse.Acknowledged {
			return nil
		} else {
			return fmt.Errorf("Delete operation was not acknowledged")
		}
	} else {
		if detailedError := errResponse.Error(); detailedError != nil {
			return detailedError
		} else {
			return err
		}
	}
}

func (self *ElasticsearchClient) Update() {

}

func (self *ElasticsearchClient) CreateIndex(index string, definition dal.Collection) error {
	createIndex := CreateIndexRequest{}
	ackResponse := AckResponse{}
	errResponse := ErrorResponse{}

	if v, ok := definition.Properties[`settings`]; ok {
		switch v.(type) {
		case map[string]interface{}:
			vMap := v.(map[string]interface{})
			createIndex.Settings = vMap
		}
	}

	if mappings, err := self.getMappingsFromCollection(&definition); err == nil {
		createIndex.Mappings = mappings
	} else {
		return err
	}

	if err := self.Request(`PUT`, fmt.Sprintf("%s", index), &createIndex, &ackResponse, &errResponse); err == nil {
		return nil
	} else {
		if detailedError := errResponse.Error(); detailedError != nil {
			return detailedError
		} else {
			return err
		}
	}
}

func (self *ElasticsearchClient) ToPivotType(esType string) (string, error) {
	switch esType {
	case `string`:
		return `str`, nil
	case `integer`, `long`, `short`, `byte`:
		return `int`, nil
	case `float`, `double`:
		return `float`, nil
	case `boolean`:
		return `bool`, nil
	default:
		return esType, nil
	}
}

func (self *ElasticsearchClient) FromPivotType(pivotType string) (string, error) {
	switch pivotType {
	case `str`:
		return `string`, nil
	case `int`:
		return `integer`, nil
	case `float`:
		return `float`, nil
	case `bool`:
		return `boolean`, nil
	default:
		return ``, fmt.Errorf("Unsupported field data type '%s' for %T", pivotType, self)
	}
}

func (self *ElasticsearchClient) getMappingsFromCollection(definition *dal.Collection) (map[string]interface{}, error) {
	mappings := make(map[string]interface{})
	docType := DEFAULT_ES_DOCUMENT_TYPE

	if v, ok := definition.Properties[`type`]; ok {
		switch v.(type) {
		case string:
			docType = v.(string)
		}
	}

	for _, field := range definition.Fields {
		var esType string

		if t, err := self.FromPivotType(field.Type); err == nil {
			esType = t
		} else {
			return nil, err
		}

		var mappingDef map[string]interface{}

		if field.Properties != nil {
			mappingDef = field.Properties
		} else {
			mappingDef = make(map[string]interface{})
		}

		mappingDef[`type`] = esType

		mappings[field.Name] = mappingDef
	}

	return map[string]interface{}{
		docType: map[string]interface{}{
			`properties`: mappings,
		},
	}, nil
}

func (self *ElasticsearchClient) IndexDocument(index string, docType string, id string, data map[string]interface{}) (Document, error) {
	indexResponse := Document{}
	errResponse := ErrorResponse{}

	if err := self.Request(`PUT`, fmt.Sprintf("%s/%s/%s", index, docType, id), &data, &indexResponse, &errResponse); err == nil {
		if indexResponse.Shards.Failed > 0 {
			return indexResponse, fmt.Errorf("Indexing document encountered %d failures", indexResponse.Shards.Failed)
		} else {
			return indexResponse, nil
		}
	} else {
		if detailedError := errResponse.Error(); detailedError != nil {
			return indexResponse, detailedError
		} else {
			return indexResponse, err
		}
	}
}

func (self *ElasticsearchClient) BulkExecute(operation BulkOperation) error {
	bulkResponse := BulkResponse{}
	errResponse := make(map[string]interface{})

	if lines, err := operation.GetRequestPayload(); err == nil {
		data := strings.Join(lines, "\n")

		if err := self.Request(`POST`, `_bulk`, &data, &bulkResponse, &errResponse); err == nil {
			if len(errResponse) == 0 {
				return nil
			} else {
				return fmt.Errorf("Encountered: %+v", errResponse)
			}
		} else {
			return err
		}
	} else {
		return err
	}
}
