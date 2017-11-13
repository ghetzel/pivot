package backends

type ConnectOptions struct {
	Indexer            string                 `json:"indexer"`
	AdditionalIndexers []string               `json:"additional_indexers"`
	Region             string                 `json:"region,omitempty"`
	Properties         map[string]interface{} `json:"properties"`
}
