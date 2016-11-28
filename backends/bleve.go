package backends

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"path"
	"strings"
)

type BleveIndexer struct {
	Indexer
	conn       dal.ConnectionString
	parent     Backend
	indexCache map[string]bleve.Index
}

func NewBleveIndexer(connection dal.ConnectionString) *BleveIndexer {
	return &BleveIndexer{
		conn:       connection,
		indexCache: make(map[string]bleve.Index),
	}
}

func (self *BleveIndexer) Initialize(parent Backend) error {
	self.parent = parent

	return nil
}

func (self *BleveIndexer) Index(collection string, records *dal.RecordSet) error {
	if index, err := self.getIndexForCollection(collection); err == nil {
		batch := index.NewBatch()

		for _, record := range records.Records {
			if err := batch.Index(string(record.ID), record.Fields); err != nil {
				return err
			}
		}

		return index.Batch(batch)
	} else {
		return err
	}
}

func (self *BleveIndexer) Query(collection string, filter filter.Filter) (*dal.RecordSet, error) {
	return nil, fmt.Errorf("%T: NI", self)
}

func (self *BleveIndexer) QueryString(collection string, filterString string) (*dal.RecordSet, error) {
	return DefaultQueryString(self, collection, filterString)
}

func (self *BleveIndexer) Remove(collection string, ids []dal.Identity) error {
	if index, err := self.getIndexForCollection(collection); err == nil {
		batch := index.NewBatch()

		for _, id := range ids {
			batch.Delete(string(id))
		}

		return index.Batch(batch)
	} else {
		return err
	}
}

func (self *BleveIndexer) getIndexForCollection(collection string) (bleve.Index, error) {
	if v, ok := self.indexCache[collection]; ok {
		return v, nil
	} else {
		var index bleve.Index
		mapping := bleve.NewIndexMapping()

		switch self.conn.Dataset() {
		case `/memory`:
			if ix, err := bleve.NewMemOnly(mapping); err == nil {
				index = ix
			} else {
				return nil, err
			}
		default:
			indexPath := self.conn.Dataset()
			baseToReplace := path.Base(indexPath)
			baseExt := path.Ext(baseToReplace)

			indexPath = strings.TrimSuffix(indexPath, baseToReplace) + fmt.Sprintf("%s-%s.%s",
				strings.TrimSuffix(baseToReplace, baseExt),
				collection,
				baseExt)

			if ix, err := bleve.Open(indexPath); err == nil {
				index = ix
			} else if ix, err := bleve.New(indexPath, mapping); err == nil {
				index = ix
			} else {
				return nil, err
			}
		}

		self.indexCache[collection] = index
		return index, nil
	}
}
