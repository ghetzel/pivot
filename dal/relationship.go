package dal

type Relationship struct {
	Keys           interface{} `json:"key"`
	Collection     *Collection `json:"-"`
	CollectionName string      `json:"collection,omitempty"`
	Fields         []string    `json:"fields,omitempty"`
}

func (self *Relationship) RelatedCollectionName() string {
	if self.Collection != nil {
		return self.Collection.Name
	} else {
		return self.CollectionName
	}
}
