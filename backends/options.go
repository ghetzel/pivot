package backends

type ConnectOptions struct {
	Indexer            string            `json:"indexer"`
	AdditionalIndexers map[string]string `json:"additional_indexers"`
}
