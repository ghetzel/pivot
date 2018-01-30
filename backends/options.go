package backends

type ConnectOptions struct {
	Indexer            string   `json:"indexer"`
	AdditionalIndexers []string `json:"additional_indexers"`
	SkipInitialize     bool     `json:"skip_initialize"`
}
