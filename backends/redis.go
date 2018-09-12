package backends

import (
	"fmt"
	"sync"
	"time"

	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/gomodule/redigo/redis"
)

type RedisBackend struct {
	Backend
	cs                    dal.ConnectionString
	conn                  redis.Conn
	registeredCollections sync.Map
	indexer               Indexer
}

func NewRedisBackend(connection dal.ConnectionString) Backend {
	return &RedisBackend{
		cs: connection,
	}
}

func (self *RedisBackend) GetConnectionString() *dal.ConnectionString {
	return &self.cs
}

func (self *RedisBackend) Ping(timeout time.Duration) error {
	errchan := make(chan error)

	go func() {
		if _, err := self.conn.Do(`PING`); err == nil {
			errchan <- nil
		} else {
			errchan <- fmt.Errorf("Backend unavailable: %v", err)
		}
	}()

	select {
	case err := <-errchan:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("Backend unavailable: timed out after waiting %v", timeout)
	}
}

func (self *RedisBackend) RegisterCollection(collection *dal.Collection) {
	self.registeredCollections.Store(collection.Name, collection)
}

func (self *RedisBackend) SetIndexer(indexConnString dal.ConnectionString) error {
	if indexer, err := MakeIndexer(indexConnString); err == nil {
		self.indexer = indexer
		return nil
	} else {
		return err
	}
}

func (self *RedisBackend) Initialize() error {
	if err := self.indexer.IndexInitialize(self); err != nil {
		return err
	}

	return nil
}

func (self *RedisBackend) Insert(collectionName string, recordset *dal.RecordSet) error {
	return fmt.Errorf("Not Implemented")
}

func (self *RedisBackend) Exists(name string, id interface{}) bool {
	return false
}

func (self *RedisBackend) Retrieve(name string, id interface{}, fields ...string) (*dal.Record, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (self *RedisBackend) Update(name string, recordset *dal.RecordSet, target ...string) error {
	return fmt.Errorf("Not Implemented")
}

func (self *RedisBackend) Delete(name string, ids ...interface{}) error {
	return fmt.Errorf("Not Implemented")
}

func (self *RedisBackend) WithSearch(collection *dal.Collection, filters ...*filter.Filter) Indexer {
	return self.indexer
}

func (self *RedisBackend) WithAggregator(collection *dal.Collection) Aggregator {
	return nil
}

func (self *RedisBackend) ListCollections() ([]string, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (self *RedisBackend) CreateCollection(definition *dal.Collection) error {
	return fmt.Errorf("Not Implemented")
}

func (self *RedisBackend) DeleteCollection(name string) error {
	return fmt.Errorf("Not Implemented")
}

func (self *RedisBackend) GetCollection(name string) (*dal.Collection, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (self *RedisBackend) Flush() error {
	return nil
}
