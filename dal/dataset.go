package dal

type Dataset struct {
	Type                          string                 `json:"type"`
	Name                          string                 `json:"dataset"`
	Addresses                     []string               `json:"addresses"`
	Options                       map[string]interface{} `json:"options,omitempty"`
	InheritedProperties           map[string]interface{} `json:"inherited_properties,omitempty"`
	Metadata                      map[string]interface{} `json:"metadata,omitempty"`
	Collections                   []Collection           `json:"collections"`
	FieldsUnordered               bool                   `json:"fields_unordered"`
	SkipFieldLength               bool                   `json:"skip_field_length"`
	MandatoryCollectionProperties []string               `json:"mandatory_collection_properties"`
	MandatoryFieldProperties      []string               `json:"mandatory_field_properties"`
}

func (self *Dataset) MakeCollection(name string) Collection {
	collection := Collection{
		Dataset:    self,
		Name:       name,
		Fields:     make([]Field, 0),
		Properties: make(map[string]interface{}),
	}

	if self.InheritedProperties != nil {
		for k, v := range self.InheritedProperties {
			collection.Properties[k] = v
		}
	}

	return collection
}

func (self *Dataset) GetCollection(name string) (Collection, bool) {
	for _, collection := range self.Collections {
		if collection.Name == name {
			return collection, true
		}
	}

	return Collection{}, false
}
