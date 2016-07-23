package elasticsearch

import (
	"encoding/json"
	"fmt"
	"github.com/fatih/structs"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
	"strings"
)

type ServerVersion struct {
	Number         string `json:"number"`
	BuildHash      string `json:"build_hash"`
	BuildTimestamp string `json:"build_timestamp"`
	BuildSnapshot  bool   `json:"build_snapshot"`
	LuceneVersion  string `json:"lucene_version"`
}

type ServerStatus struct {
	Name        string        `json:"name"`
	ClusterName string        `json:"cluster_name"`
	Version     ServerVersion `json:"version"`
}

type ClusterHealth struct {
	Name                        string  `json:"cluster_name" structs:"cluster_name"`
	Status                      string  `json:"status" structs:"status"`
	TimedOut                    bool    `json:"timed_out" structs:"timed_out"`
	NumberOfNodes               int     `json:"number_of_nodes" structs:"number_of_nodes"`
	NumberOfDataNodes           int     `json:"number_of_data_nodes" structs:"number_of_data_nodes"`
	ActivePrimaryShards         int     `json:"active_primary_shards" structs:"active_primary_shards"`
	ActiveShards                int     `json:"active_shards" structs:"active_shards"`
	RelocatingShards            int     `json:"relocating_shards" structs:"relocating_shards"`
	InitializingShards          int     `json:"initializing_shards" structs:"initializing_shards"`
	UnassignedShards            int     `json:"unassigned_shards" structs:"unassigned_shards"`
	DelayedUnassignedShards     int     `json:"delayed_unassigned_shards" structs:"delayed_unassigned_shards"`
	NumberOfPendingTasks        int     `json:"number_of_pending_tasks" structs:"number_of_pending_tasks"`
	NumberOfInFlightFetch       int     `json:"number_of_in_flight_fetch" structs:"number_of_in_flight_fetch"`
	TaskMaxWaitingInQueueMillis int     `json:"task_max_waiting_in_queue_millis" structs:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercentAsNumber float32 `json:"active_shards_percent_as_number" structs:"active_shards_percent_as_number"`
}

func (self *ClusterHealth) ToMap() map[string]interface{} {
	return structs.Map(self)
}

type ShardStats struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type IndexStats struct {
	Shards  ShardStats             `json:"_shards"`
	Indices map[string]interface{} `json:"indices"`
}

type Mapping struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

type IndexMapping struct {
	IndexName string             `json:"index"`
	Mappings  map[string]Mapping `json:"mappings"`
}

type AckResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

type SearchRequest struct {
	Index   string                 `json:"index"`
	Type    string                 `json:"type"`
	Query   map[string]interface{} `json:"query"`
	Options map[string]interface{} `json:"options"`
}

func NewSearchRequestFromFilter(index string, docType string, f filter.Filter) (*SearchRequest, error) {
	generator := generators.NewElasticsearchGenerator()

	if queryData, err := filter.Render(generator, index, f); err == nil {
		queryBody := make(map[string]interface{})

		if err := json.Unmarshal(queryData, &queryBody); err == nil {
			options := make(map[string]interface{})

			options[`_source`] = false

			//  _source processing only happens if we want all fields, or are only looking
			//  at metadata fields
			if len(f.Fields) == 0 {
				options[`_source`] = true
			} else {
				for _, field := range f.Fields {
					if !strings.HasPrefix(field, `_`) {
						options[`_source`] = true
						break
					}
				}
			}

			// if we want the _version field, set the flag
			options[`_version`] = sliceutil.ContainsString(f.Fields, `_version`)

			//  limit result size
			if v, ok := f.Options[`page_size`]; ok {
				if i, err := stringutil.ConvertToInteger(v); err == nil {
					options[`size`] = i
				} else {
					return nil, fmt.Errorf("Invalid 'page_size' parameter: %v", err)
				}
			}

			//  offset results
			if v, ok := f.Options[`offset`]; ok {
				if i, err := stringutil.ConvertToInteger(v); err == nil {
					options[`from`] = i
				} else {
					return nil, fmt.Errorf("Invalid 'offset' parameter: %v", err)
				}
			}

			return &SearchRequest{
				Index: index,
				Type:  docType,
				Query: map[string]interface{}{
					`bool`: map[string]interface{}{
						`must`: map[string]interface{}{
							`match_all`: map[string]interface{}{},
						},
						`filter`: queryBody,
					},
				},
				Options: options,
			}, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

type Document struct {
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	ID      string                 `json:"_id"`
	Score   int                    `json:"_score"`
	Version int                    `json:"_version"`
	Source  map[string]interface{} `json:"_source"`
}

type SearchResponseHits struct {
	Total    int        `json:"total"`
	MaxScore int        `json:"max_score"`
	Hits     []Document `json:"hits"`
}

type SearchResponse struct {
	Took     int                `json:"took"`
	TimedOut bool               `json:"timed_out`
	Shards   ShardStats         `json:"shards"`
	Hits     SearchResponseHits `json:"hits"`
}
