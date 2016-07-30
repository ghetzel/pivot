package elasticsearch

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/util"
	"math"
	"strings"
)

type ElasticsearchClient struct {
	*util.MultiClient
}

func NewClient() *ElasticsearchClient {
	client := util.NewMultiClient()

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

func (self *ElasticsearchClient) DeleteByQuery() {

}

func (self *ElasticsearchClient) Update() {

}

func (self *ElasticsearchClient) CreateIndex(index string) {

}
