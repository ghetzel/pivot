package dal

type Relationship struct {
	Keys           interface{} `json:"key"`
	Collection     *Collection `json:"-"`
	CollectionName string      `json:"collection,omitempty"`
	Fields         []string    `json:"fields,omitempty"`
}
