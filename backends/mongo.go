package backends

import (
	"fmt"
	"sync"
	"time"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var DefaultConnectTimeout = 10 * time.Second
var MongoIdentityField = `_id`

type MongoBackend struct {
	Backend
	Indexer
	conn                  *dal.ConnectionString
	registeredCollections sync.Map
	session               *mgo.Session
	db                    *mgo.Database
	indexer               Indexer
}

func NewMongoBackend(connection dal.ConnectionString) Backend {
	backend := &MongoBackend{
		conn: &connection,
	}

	backend.indexer = backend
	return backend
}

func (self *MongoBackend) Initialize() error {
	cstring := fmt.Sprintf("%s://%s/%s", self.conn.Backend(), self.conn.Host(), self.conn.Dataset())

	if session, err := mgo.DialWithTimeout(cstring, DefaultConnectTimeout); err == nil {
		self.session = session

		if u, p, ok := self.conn.Credentials(); ok {
			credentials := &mgo.Credential{
				Username:    u,
				Password:    p,
				Source:      self.conn.OptString(`authdb`, ``),
				Service:     self.conn.OptString(`authService`, ``),
				ServiceHost: self.conn.OptString(`authHost`, ``),
			}

			switch self.conn.Protocol() {
			case `scram`, `scram-sha1`:
				credentials.Mechanism = `SCRAM-SHA-1`
			case `cr`:
				credentials.Mechanism = `MONGODB-CR`
			}

			if err := self.session.Login(credentials); err != nil {
				return fmt.Errorf("auth failed: %v", err)
			}
		}

		self.db = session.DB(self.conn.Dataset())

		if self.conn.OptBool(`autoregister`, DefaultAutoregister) {
			if names, err := self.db.CollectionNames(); err == nil {
				for _, name := range names {
					collection := dal.NewCollection(name)
					self.RegisterCollection(collection)
				}
			} else {
				return err
			}
		}

		return nil
	} else {
		return err
	}
}

func (self *MongoBackend) SetIndexer(indexConnString dal.ConnectionString) error {
	if indexer, err := MakeIndexer(indexConnString); err == nil {
		self.indexer = indexer
		return nil
	} else {
		return err
	}
}

func (self *MongoBackend) RegisterCollection(collection *dal.Collection) {
	if collection != nil {
		collection.IdentityField = MongoIdentityField
		self.registeredCollections.Store(collection.Name, collection)
		log.Debugf("[%T] register collection %v", self, collection.Name)
	}
}

func (self *MongoBackend) GetConnectionString() *dal.ConnectionString {
	return self.conn
}

func (self *MongoBackend) Exists(name string, id interface{}) bool {
	if collection, err := self.GetCollection(name); err == nil {
		if n, err := self.db.C(collection.Name).FindId(self.getId(id)).Count(); err == nil && n == 1 {
			return true
		}
	}

	return false
}

func (self *MongoBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	if collection, err := self.GetCollection(name); err == nil {
		var data map[string]interface{}

		if err := self.db.C(collection.Name).FindId(self.getId(id)).One(&data); err == nil {
			return self.recordFromResult(collection, data, fields...)
		} else if err == mgo.ErrNotFound {
			return nil, fmt.Errorf("Record %v does not exist", id)
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *MongoBackend) Insert(name string, records *dal.RecordSet) error {
	if collection, err := self.GetCollection(name); err == nil {
		for _, record := range records.Records {
			if _, err := collection.MakeRecord(record); err == nil {
				self.normalizeRecordValues(record)

				data := record.Fields

				if record.ID != nil {
					data[MongoIdentityField] = self.getId(record.ID)
				} else {
					delete(data, MongoIdentityField)
				}

				if err := self.db.C(collection.Name).Insert(&data); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func (self *MongoBackend) Update(name string, records *dal.RecordSet, target ...string) error {
	if collection, err := self.GetCollection(name); err == nil {
		for _, record := range records.Records {
			if _, err := collection.MakeRecord(record); err == nil {
				self.normalizeRecordValues(record)

				if record.ID == nil {
					return fmt.Errorf("Cannot update record without an ID")
				} else {
					if err := self.db.C(collection.Name).UpdateId(self.getId(record.ID), record.Fields); err != nil {
						return err
					}
				}
			} else {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func (self *MongoBackend) Delete(name string, ids ...interface{}) error {
	if collection, err := self.GetCollection(name); err == nil {
		for _, id := range ids {
			if err := self.db.C(collection.Name).RemoveId(self.getId(id)); err != nil {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func (self *MongoBackend) CreateCollection(definition *dal.Collection) error {
	if _, err := self.GetCollection(definition.Name); err == nil {
		return fmt.Errorf("Collection %v already exists", definition.Name)
	} else if dal.IsCollectionNotFoundErr(err) {
		if err := self.db.C(definition.Name).Create(&mgo.CollectionInfo{}); err == nil {
			self.registeredCollections.Store(definition.Name, definition)
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *MongoBackend) DeleteCollection(name string) error {
	if collection, err := self.GetCollection(name); err == nil {
		if err := self.db.C(collection.Name).DropCollection(); err == nil {
			self.registeredCollections.Delete(collection.Name)
			return nil
		} else {
			return err
		}
	} else if dal.IsCollectionNotFoundErr(err) {
		return nil
	} else {
		return err
	}
}

func (self *MongoBackend) ListCollections() ([]string, error) {
	return maputil.StringKeys(&self.registeredCollections), nil
}

func (self *MongoBackend) GetCollection(name string) (*dal.Collection, error) {
	if cI, ok := self.registeredCollections.Load(name); ok {
		if collection, ok := cI.(*dal.Collection); ok {
			return collection, nil
		} else {
			return nil, fmt.Errorf("Collection type error: got %T, want *dal.Collection", cI)
		}
	} else {
		return nil, dal.CollectionNotFound
	}
}

func (self *MongoBackend) WithSearch(collection string, filters ...*filter.Filter) Indexer {
	return self.indexer
}

func (self *MongoBackend) WithAggregator(collection string) Aggregator {
	return self
}

func (self *MongoBackend) Flush() error {
	return nil
}

func (self *MongoBackend) normalizeRecordValues(record *dal.Record) {
	for name, value := range record.Fields {
		switch value.(type) {
		case bson.ObjectId:
			record.Fields[name] = value.(bson.ObjectId).Hex()
		}
	}
}

func (self *MongoBackend) recordFromResult(collection *dal.Collection, data map[string]interface{}, fields ...string) (*dal.Record, error) {
	if dataId, ok := data[MongoIdentityField]; ok {
		record := dal.NewRecord(
			collection.ConvertValue(MongoIdentityField, stringutil.Autotype(
				self.fromId(dataId),
			)),
		)

		for k, v := range data {
			v = self.fromId(v)

			if _, ok := collection.GetField(k); ok || len(collection.Fields) == 0 {
				if len(fields) == 0 || sliceutil.ContainsString(fields, k) {
					record.Set(k, v)
				}
			}
		}

		delete(record.Fields, MongoIdentityField)

		// do this AFTER populating the record's fields from the database
		if err := record.Populate(record, collection); err != nil {
			return nil, fmt.Errorf("error populating record: %v", err)
		}

		self.normalizeRecordValues(record)

		return record, nil
	} else {
		return nil, fmt.Errorf("Could not locate identity field %s", MongoIdentityField)
	}
}

func (self *MongoBackend) getId(in interface{}) interface{} {
	switch in.(type) {
	case string:
		if bson.IsObjectIdHex(in.(string)) {
			return bson.ObjectIdHex(in.(string))
		}
	}

	return in
}

func (self *MongoBackend) fromId(in interface{}) interface{} {

	switch in.(type) {
	case bson.ObjectId:
		return in.(bson.ObjectId).Hex()
	}

	return in
}
