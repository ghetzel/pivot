package dummy

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/patterns"
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger(`backends`)

type DummyBackend struct {
	backends.Backend
	patterns.IRecordAccessPattern
	Connected       bool
	connectAt       time.Time
	disconnectDelay time.Duration
	connectDelay    time.Duration
}

const (
	DEFAULT_DUMMY_CONNECT_DELAY    = 3000
	DEFAULT_DUMMY_DISCONNECT_DELAY = 13000
)

func New(name string, config dal.Dataset) *DummyBackend {
	config.Collections = make([]dal.Collection, 0)

	return &DummyBackend{
		Backend: backends.Backend{
			Name:          name,
			Dataset:       config,
			SchemaRefresh: time.Millisecond * 1000,
		},
	}
}

func (self *DummyBackend) SetConnected(c bool) {
	self.Connected = c

	if c {
		log.Infof("Backend %s is CONNECTED", self.GetName())
	} else {
		log.Warningf("Backend %s is DISCONNECTED", self.GetName())
	}
}

func (self *DummyBackend) IsConnected() bool {
	return self.Connected
}

func (self *DummyBackend) Disconnect() {
	self.SetConnected(false)
}

func (self *DummyBackend) Connect() error {
	if v, ok := self.Dataset.Options[`connect_delay`]; ok {
		if value, err := stringutil.ConvertToInteger(v); err == nil {
			if value > 0 {
				self.connectDelay = (time.Millisecond * time.Duration(value))
			}
		}
	}

	if v, ok := self.Dataset.Options[`disconnect_delay`]; ok {
		if value, err := stringutil.ConvertToInteger(v); err == nil {
			if value > 0 {
				self.disconnectDelay = (time.Millisecond * time.Duration(value))
			}
		}
	}

	time.Sleep(self.connectDelay)
	self.connectAt = time.Now()
	self.SetConnected(true)

	return self.Finalize(self)
}

func (self *DummyBackend) Refresh() error {
	if time.Now().After(self.connectAt.Add(self.disconnectDelay)) {
		return fmt.Errorf("Dummy backend %s is down now", self.GetName())
	}

	return nil
}

func (self *DummyBackend) Info() map[string]interface{} {
	return map[string]interface{}{}
}

func (self *DummyBackend) GetPatternType() patterns.PatternType {
	return patterns.RecordPattern
}

func (self *DummyBackend) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		`type`:      `dummy`,
		`connected`: self.IsConnected(),
		`available`: self.IsAvailable(),
	}
}

func (self *DummyBackend) ReadDatasetSchema() *dal.Dataset {
	return self.GetDataset()
}

func (self *DummyBackend) ReadCollectionSchema(collectionName string) (dal.Collection, bool) {
	for _, collection := range self.Dataset.Collections {
		if collection.Name == collectionName {
			return collection, true
		}
	}

	return dal.Collection{}, false
}

func (self *DummyBackend) UpdateCollectionSchema(action dal.CollectionAction, collectionName string, definition dal.Collection) error {
	return fmt.Errorf("Not implemented")
}

func (self *DummyBackend) DeleteCollectionSchema(collectionName string) error {
	return fmt.Errorf("Not implemented")
}

func (self *DummyBackend) GetRecords(collectionName string, f filter.Filter) (*dal.RecordSet, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (self *DummyBackend) InsertRecords(collectionName string, f filter.Filter, payload *dal.RecordSet) error {
	return fmt.Errorf("Not implemented")
}

func (self *DummyBackend) UpdateRecords(collectionName string, f filter.Filter, payload *dal.RecordSet) error {
	return fmt.Errorf("Not implemented")
}

func (self *DummyBackend) DeleteRecords(collectionName string, f filter.Filter) error {
	return fmt.Errorf("Not implemented")
}
