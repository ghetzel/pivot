package pivot

import (
	"github.com/ghetzel/pivot/dal"
)

func (self *Backend) InsertRecords(collectionName string, recordset *dal.RecordSet) error {
	path := self.GetPath(`query`, collectionName)

	if _, err := self.Client.Call(`POST`, path, recordset); err == nil {
		return nil
	} else {
		return err
	}
}
