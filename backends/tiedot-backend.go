package backends

import (
	"fmt"
	"strings"

	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
)

type TiedotBackend struct {
	Backend
	conn                  dal.ConnectionString
	root                  string
	db                    *db.DB
	indexer               Indexer
	aggregator            map[string]Aggregator
	options               ConnectOptions
	registeredCollections map[string]*dal.Collection
}

func NewTiedotBackend(connection dal.ConnectionString) Backend {
	return &TiedotBackend{
		conn:                  connection,
		aggregator:            make(map[string]Aggregator),
		registeredCollections: make(map[string]*dal.Collection),
	}
}

func (self *TiedotBackend) GetConnectionString() *dal.ConnectionString {
	return &self.conn
}

func (self *TiedotBackend) RegisterCollection(collection *dal.Collection) {
	self.registeredCollections[collection.Name] = collection
}

func (self *TiedotBackend) SetOptions(options ConnectOptions) {
	self.options = options
}

func (self *TiedotBackend) Initialize() error {
	self.root = self.conn.Dataset()

	if self.root == `` {
		return fmt.Errorf("Must specify a root directory")
	} else if v, err := pathutil.ExpandUser(self.root); err == nil {
		self.root = v
	} else {
		return err
	}

	if db, err := db.OpenDB(self.root); err == nil {
		self.db = db
	} else {
		return err
	}

	// setup indexer
	if indexConnString := self.options.Indexer; indexConnString != `` {
		if ics, err := dal.ParseConnectionString(indexConnString); err == nil {
			if indexer, err := MakeIndexer(ics); err == nil {
				if err := indexer.IndexInitialize(self); err == nil {
					self.indexer = indexer
				}
			}
		}
	}

	return nil
}

func (self *TiedotBackend) Insert(collectionName string, recordset *dal.RecordSet) error {
	return self.upsertRecordset(collectionName, recordset, true)
}

func (self *TiedotBackend) Exists(name string, idI interface{}) bool {
	if collection, ok := self.registeredCollections[name]; ok {
		if col := self.db.Use(collection.Name); col != nil {
			if id, err := stringutil.ConvertToInteger(idI); err == nil {
				if _, err := col.Read(int(id)); err == nil {
					return true
				}
			}
		}
	}

	return false
}

func (self *TiedotBackend) Retrieve(name string, idI interface{}, fields ...string) (*dal.Record, error) {
	if collection, ok := self.registeredCollections[name]; ok {
		if col := self.db.Use(collection.Name); col != nil {
			if id, err := stringutil.ConvertToInteger(idI); err == nil {
				if document, err := col.Read(int(id)); err == nil {
					record := dal.NewRecord(id)

					if err := record.Populate(record, collection); err != nil {
						return nil, err
					}

					for k, v := range document {
						record.Set(k, v)
					}

					return record, nil
				} else {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("Backend %T only supports integer IDs, got %T", self, idI)
			}
		} else {
			return nil, dal.CollectionNotFound
		}
	} else {
		return nil, dal.CollectionNotFound
	}
}

func (self *TiedotBackend) Update(name string, recordset *dal.RecordSet, target ...string) error {
	return self.upsertRecordset(name, recordset, false)
}

func (self *TiedotBackend) Delete(name string, ids ...interface{}) error {
	if collection, ok := self.registeredCollections[name]; ok {
		if col := self.db.Use(collection.Name); col != nil {
			for _, idI := range ids {
				if id, err := stringutil.ConvertToInteger(idI); err == nil {
					if err := col.Delete(int(id)); err != nil {
						if dberr.Type(err) != dberr.ErrorNoDoc {
							return err
						}
					}
				} else {
					return err
				}
			}

			if search := self.WithSearch(collection.Name); search != nil {
				// remove documents from index
				return search.IndexRemove(collection.Name, ids)
			}
		}
	}

	return nil
}

func (self *TiedotBackend) WithSearch(collectionName string, filters ...filter.Filter) Indexer {
	return self.indexer
}

func (self *TiedotBackend) WithAggregator(collectionName string) Aggregator {
	return nil
}

func (self *TiedotBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(self.registeredCollections), nil
}

func (self *TiedotBackend) CreateCollection(definition *dal.Collection) error {
	if err := self.db.Use(definition.Name); err == nil {
		if err := self.db.Create(definition.Name); err == nil {
			self.RegisterCollection(definition)
			return nil
		} else {
			return err
		}
	} else {
		return fmt.Errorf("Collection %q already exists", definition.Name)
	}
}

func (self *TiedotBackend) DeleteCollection(collectionName string) error {
	if err := self.db.Drop(collectionName); err == nil {
		return nil
	} else if strings.HasSuffix(err.Error(), ` does not exist`) {
		return dal.CollectionNotFound
	} else {
		return err
	}
}

func (self *TiedotBackend) GetCollection(name string) (*dal.Collection, error) {
	if collection, ok := self.registeredCollections[name]; ok {
		if col := self.db.Use(collection.Name); col != nil {
			return collection, nil
		}
	}

	return nil, dal.CollectionNotFound
}

func (self *TiedotBackend) Flush() error {
	if self.indexer != nil {
		return self.indexer.FlushIndex()
	}

	return nil
}

func (self *TiedotBackend) upsertRecordset(name string, recordset *dal.RecordSet, isCreate bool) error {
	if collection, ok := self.registeredCollections[name]; ok {
		for _, record := range recordset.Records {
			if r, err := collection.MakeRecord(record); err == nil {
				record = r
			} else {
				return err
			}

			if isCreate && record.ID != nil {
				return fmt.Errorf("Backend %T does not allow explicitly setting IDs", self)
			}

			if col := self.db.Use(collection.Name); col != nil {
				if isCreate {
					if id, err := col.Insert(record.Fields); err == nil {
						record.ID = int64(id)
					} else {
						return err
					}
				} else {
					if id, err := stringutil.ConvertToInteger(record.ID); err == nil {
						if err := col.Update(int(id), record.Fields); err != nil {
							return err
						}
					} else {
						return fmt.Errorf("Backend %T only supports integer IDs, got %T", self, record.ID)
					}
				}
			} else {
				return dal.CollectionNotFound
			}
		}

		if search := self.WithSearch(collection.Name); search != nil {
			if err := search.Index(collection.Name, recordset); err != nil {
				return err
			}
		}
	} else {
		return dal.CollectionNotFound
	}

	return nil
}
