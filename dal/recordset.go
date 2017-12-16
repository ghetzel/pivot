package dal

type RecordSet struct {
	ResultCount    int64                  `json:"result_count"`
	Page           int                    `json:"page,omitempty"`
	TotalPages     int                    `json:"total_pages,omitempty"`
	RecordsPerPage int                    `json:"records_per_page,omitempty"`
	Records        []*Record              `json:"records"`
	Options        map[string]interface{} `json:"options"`
	KnownSize      bool                   `json:"known_size"`
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

func (self *RecordSet) Append(other *RecordSet) *RecordSet {
	for _, record := range other.Records {
		self.Push(record)
	}

	return self
}

func (self *RecordSet) GetRecord(index int) (*Record, bool) {
	if index < len(self.Records) {
		return self.Records[index], true
	}

	return nil, false
}

func (self *RecordSet) Pluck(field string, fallback ...interface{}) []interface{} {
	rv := make([]interface{}, 0)

	for _, record := range self.Records {
		rv = append(rv, record.Get(field, fallback...))
	}

	return rv
}

func (self *RecordSet) IsEmpty() bool {
	if self.ResultCount == 0 {
		return true
	} else {
		return false
	}
}
