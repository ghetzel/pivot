package dal

type Identity string

type Record struct {
	ID     Identity               `json:"id"`
	Fields map[string]interface{} `json:"fields,omitempty"`
	Data   []byte                 `json:"data,omitempty"`
}

func NewRecord(id Identity) *Record {
	return &Record{
		ID:     id,
		Fields: make(map[string]interface{}),
		Data:   make([]byte, 0),
	}
}

type RecordSet struct {
	ResultCount uint64                 `json:"result_count"`
	Page        int                    `json:"page"`
	TotalPages  int                    `json:"total_pages"`
	Records     []*Record              `json:"records"`
	Options     map[string]interface{} `json:"options`
}

func NewRecordSet(records ...*Record) *RecordSet {
	return &RecordSet{
		Records: records,
		Options: make(map[string]interface{}),
	}
}

func (self *RecordSet) Push(record *Record) *RecordSet {
	self.Records = append(self.Records, record)
	self.ResultCount = self.ResultCount + 1
	return self
}

func (self *RecordSet) GetRecord(index int) (*Record, bool) {
	if index < len(self.Records) {
		return self.Records[index], true
	}

	return nil, false
}

func (self *RecordSet) ToMap(index int) (map[string]interface{}, bool) {
	if record, ok := self.GetRecord(index); ok {
		return record.Fields, true
	}

	return make(map[string]interface{}), false
}

func (self *RecordSet) IsEmpty() bool {
	if self.ResultCount == 0 {
		return true
	} else {
		return false
	}
}
