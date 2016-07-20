package dal

type CollectionAction int

const (
	SchemaVerify  CollectionAction = 0
	SchemaCreate  CollectionAction = 1
	SchemaExpand  CollectionAction = 2
	SchemaRemove  CollectionAction = 3
	SchemaEnforce CollectionAction = 4
)

type Collection struct {
	Dataset    *Dataset               `json:"-"`
	Name       string                 `json:"name"`
	Fields     []Field                `json:"fields"`
	Properties map[string]interface{} `json:"properties"`
}

func (self *Collection) GetField(name string) (Field, bool) {
	for _, field := range self.Fields {
		if field.Name == name {
			return field, true
		}
	}

	return Field{}, false
}
