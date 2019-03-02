package dal

import (
	"fmt"
	"reflect"

	"github.com/ghetzel/go-stockutil/typeutil"
)

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

func (self *RecordSet) GetRecordByID(id interface{}) (*Record, bool) {
	for _, record := range self.Records {
		if typeutil.String(record.ID) == typeutil.String(id) {
			return record, true
		}
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

// Takes a slice of structs or maps and fills it with instances populated by the records in this RecordSet
// in accordance with the types specified in the given collection definition, as well as which
// fields are available in the given struct.
func (self *RecordSet) PopulateFromRecords(into interface{}, schema *Collection) error {
	vInto := reflect.ValueOf(into)

	// get value pointed to if we were given a pointer
	if vInto.Kind() == reflect.Ptr {
		vInto = vInto.Elem()
	} else {
		return fmt.Errorf("Output argument must be a pointer")
	}

	// we're going to fill arrays or slices
	switch vInto.Type().Kind() {
	case reflect.Array, reflect.Slice:
		indirectResult := true

		// get the type of the underlying slice element
		sliceType := vInto.Type().Elem()

		// get the type pointed to
		if sliceType.Kind() == reflect.Ptr {
			sliceType = sliceType.Elem()
			indirectResult = false
		}

		// for each resulting record...
		for _, record := range self.Records {
			// make a new zero-valued instance of the slice type
			elem := reflect.New(sliceType)

			// if we have a registered collection, use its
			if schema != nil && schema.HasRecordType() {
				elem = reflect.ValueOf(schema.NewInstance())
			}

			// populate that type with data from this record
			if err := record.Populate(elem.Interface(), schema); err == nil {
				// if the slice elements are pointers, we can append the pointer we just created as-is
				// otherwise, we need to indirect the value and append a copy

				if indirectResult {
					vInto.Set(reflect.Append(vInto, reflect.Indirect(elem)))
				} else {
					vInto.Set(reflect.Append(vInto, elem))
				}
			} else {
				return err
			}
		}

		return nil
	case reflect.Struct:
		if rs, ok := into.(*RecordSet); ok {
			*rs = *self
			return nil
		}
	}

	return fmt.Errorf("RecordSet can only populate records into slice or array, got %T", into)
}
