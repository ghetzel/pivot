package dal

type Record struct {
	ID     string                 `json:"id"`
	Fields map[string]interface{} `json:"fields,omitempty"`
	Data   []byte                 `json:"data,omitempty"`
}

func NewRecord(id string) *Record {
	return &Record{
		ID:     id,
		Fields: make(map[string]interface{}),
	}
}

func (self *Record) Get(key string, fallback ...interface{}) interface{} {
	if v, ok := self.Fields[key]; ok {
		return v
	} else if len(fallback) > 0 {
		return fallback[0]
	} else {
		return nil
	}
}

func (self *Record) Set(key string, value interface{}) *Record {
	self.Fields[key] = value
	return self
}

func (self *Record) SetFields(values map[string]interface{}) *Record {
	for k, v := range values {
		self.Set(k, v)
	}

	return self
}

func (self *Record) SetData(data []byte) *Record {
	self.Data = data
	return self
}
