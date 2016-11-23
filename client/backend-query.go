package pivot

import (
	"fmt"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

func (self *Backend) InsertRecords(collectionName string, recordset *dal.RecordSet) error {
	path := self.GetPath(`query`, collectionName)

	if _, err := self.Client.Call(`POST`, path, recordset); err == nil {
		return nil
	} else {
		return err
	}
}

func (self *Backend) GetRecordById(collectionName string, id string) (*dal.Record, error) {
	return new(dal.Record), fmt.Errorf("Not Implemented")
}

func (self *Backend) QueryRecords(collectionName string, filter filter.Filter) (*dal.RecordSet, error) {
	return dal.NewRecordSet(), fmt.Errorf("Not Implemented")
}
