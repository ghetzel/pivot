package dal

type Dataset struct {
	Type        string                 `json:"type"`
	Name        string                 `json:"dataset"`
	Addresses   []string               `json:"addresses"`
	Options     map[string]interface{}      `json:"options,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	Collections []Collection           `json:"collections"`
}

func (self *Dataset) GetCollection(name string) (Collection, bool) {
	for _, collection := range self.Collections {
		if collection.Name == name {
			return collection, true
		}
	}

	return Collection{}, false
}
