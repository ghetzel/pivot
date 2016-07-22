package elasticsearch

import (
	"github.com/fatih/structs"
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
	Name                        string `json:"cluster_name" structs:"cluster_name"`
	Status                      string `json:"status" structs:"status"`
	TimedOut                    bool   `json:"timed_out" structs:"timed_out"`
	NumberOfNodes               int    `json:"number_of_nodes" structs:"number_of_nodes"`
	NumberOfDataNodes           int    `json:"number_of_data_nodes" structs:"number_of_data_nodes"`
	ActivePrimaryShards         int    `json:"active_primary_shards" structs:"active_primary_shards"`
	ActiveShards                int    `json:"active_shards" structs:"active_shards"`
	RelocatingShards            int    `json:"relocating_shards" structs:"relocating_shards"`
	InitializingShards          int    `json:"initializing_shards" structs:"initializing_shards"`
	UnassignedShards            int    `json:"unassigned_shards" structs:"unassigned_shards"`
	DelayedUnassignedShards     int    `json:"delayed_unassigned_shards" structs:"delayed_unassigned_shards"`
	NumberOfPendingTasks        int    `json:"number_of_pending_tasks" structs:"number_of_pending_tasks"`
	NumberOfInFlightFetch       int    `json:"number_of_in_flight_fetch" structs:"number_of_in_flight_fetch"`
	TaskMaxWaitingInQueueMillis int    `json:"task_max_waiting_in_queue_millis" structs:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercentAsNumber int    `json:"active_shards_percent_as_number" structs:"active_shards_percent_as_number"`
}

func (self *ClusterHealth) ToMap() map[string]interface{} {
	return structs.Map(self)
}

type IndexStats struct {
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
