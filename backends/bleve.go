package backends

import (
	"github.com/blevesearch/bleve"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/filter/generators"
	"path"
)

type BleveIndexer struct {
	Indexer
	conn       dal.ConnectionString
	parent     Backend
	indexCache map[string]bleve.Index
	generator  filter.IGenerator
}

func NewBleveIndexer(connection dal.ConnectionString) *BleveIndexer {
	return &BleveIndexer{
		conn:       connection,
		indexCache: make(map[string]bleve.Index),
		generator:  generators.NewBleveGenerator(),
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
			log.Debugf("Indexing %s/%+v", collection, record)

			if err := batch.Index(string(record.ID), record.Fields); err != nil {
				return err
			}
		}

		return index.Batch(batch)
	} else {
		return err
	}
}

func (self *BleveIndexer) Query(collection string, f filter.Filter) (*dal.RecordSet, error) {
	if index, err := self.getIndexForCollection(collection); err == nil {
		if bleveQSQ, err := filter.Render(self.generator, collection, f); err == nil {
			query := bleve.NewQueryStringQuery(string(bleveQSQ[:]))
			request := bleve.NewSearchRequest(query)

			log.Debugf("Querying: %+v", query)

			if results, err := index.Search(request); err == nil {
				recordset := dal.NewRecordSet()

				for _, hit := range results.Hits {
					if record, err := self.parent.GetRecordById(collection, dal.Identity(hit.ID)); err == nil {
						recordset.Push(record)
					}
				}

				return recordset, nil
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *BleveIndexer) QueryString(collection string, filterString string) (*dal.RecordSet, error) {
	return DefaultQueryString(self, collection, filterString)
}

func (self *BleveIndexer) Remove(collection string, ids []dal.Identity) error {
	if index, err := self.getIndexForCollection(collection); err == nil {
		batch := index.NewBatch()

		for _, id := range ids {
			log.Debugf("Deindexing %s/%s", collection, id)
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
			indexBaseDir := self.conn.Dataset()
			indexPath := path.Join(indexBaseDir, collection)

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
