package dummy

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger(`backends`)

type DummyBackend struct {
	backends.Backend
	Connected       bool `json:"connected"`
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
		log.Noticef("Backend %s is CONNECTED", self.GetName())
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

func (self *DummyBackend) UpdateCollectionSchema(action dal.CollectionAction, definition dal.Collection) error {
	return fmt.Errorf("Not implemented")
}

func (self *DummyBackend) DeleteCollectionSchema(collectionName string) error {
	return fmt.Errorf("Not implemented")
}

func (self *DummyBackend) GetRecords(collectionName string, f filter.Filter) (*dal.RecordSet, error) {
	return dal.NewRecordSet().Push(dal.Record{
		`id`:   1,
		`name`: `Foo`,
		`properties`: map[string]interface{}{
			`waldo`: true,
			`fred`:  false,
			`plugh`: 42,
		},
	}).Push(dal.Record{
		`id`:   2,
		`name`: `Bar`,
	}).Push(dal.Record{
		`id`:   3,
		`name`: `Baz`,
	}), nil
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
