package backends

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/patterns"
	"github.com/op/go-logging"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var log = logging.MustGetLogger(`backends`)

type PayloadType int

const (
	RequestPayload  PayloadType = 0
	ResponsePayload             = 1
)

const (
	DEFAULT_CONNECT_ATTEMPTS          = -1
	DEFAULT_CONNECT_TIMEOUT_MS        = 15000
	DEFAULT_SCHEMA_REFRESH_TIMEOUT_MS = 10000
	DEFAULT_SCHEMA_REFRESH_MAX_FAIL   = 5
)

type IBackend interface {
	Connect() error
	Disconnect()
	Finalize(IBackend) error
	GetConnectMaxAttempts() int
	GetConnectTimeout() time.Duration
	GetDataset() *dal.Dataset
	GetName() string
	GetPatternType() patterns.PatternType
	Info() map[string]interface{}
	Initialize() error
	IsAvailable() bool
	IsConnected() bool
	ProcessPayload(PayloadType, *dal.RecordSet, *http.Request) error
	Refresh() error
	RefreshInterval() time.Duration
	RefreshMaxFailures() int
	RefreshTimeout() time.Duration
	Resume()
	SetConnected(bool)
	Suspend()
}

type Backend struct {
	IBackend
	Available            bool
	Connected            bool
	ConnectMaxAttempts   int
	ConnectTimeout       time.Duration
	Dataset              dal.Dataset
	Name                 string
	SchemaRefresh        time.Duration
	SchemaRefreshMaxFail int
	SchemaRefreshTimeout time.Duration
}

func (self *Backend) Initialize() error {
	if v, ok := self.Dataset.Options[`connect_attempts`]; ok && v != `` {
		if vInt, err := strconv.ParseInt(v, 10, 32); err == nil && vInt > 0 {
			self.ConnectMaxAttempts = int(vInt)
		} else {
			self.ConnectMaxAttempts = DEFAULT_CONNECT_ATTEMPTS
		}
	} else {
		self.ConnectMaxAttempts = DEFAULT_CONNECT_ATTEMPTS
	}

	if v, ok := self.Dataset.Options[`connect_timeout`]; ok && v != `` {
		if vInt, err := strconv.ParseInt(v, 10, 32); err == nil {
			self.ConnectTimeout = time.Duration(vInt) * time.Millisecond
		} else {
			self.ConnectTimeout = time.Duration(DEFAULT_CONNECT_TIMEOUT_MS) * time.Millisecond
		}
	} else {
		self.ConnectTimeout = time.Duration(DEFAULT_CONNECT_TIMEOUT_MS) * time.Millisecond
	}

	if v, ok := self.Dataset.Options[`refresh_failures`]; ok && v != `` {
		if vInt, err := strconv.ParseInt(v, 10, 32); err == nil && vInt > 0 {
			self.SchemaRefreshMaxFail = int(vInt)
		} else {
			self.SchemaRefreshMaxFail = DEFAULT_SCHEMA_REFRESH_MAX_FAIL
		}
	} else {
		self.SchemaRefreshMaxFail = DEFAULT_SCHEMA_REFRESH_MAX_FAIL
	}

	return nil
}

//  This is called after a connection has been successfully established with the backend data store
func (self *Backend) Finalize(caller IBackend) error {
	//  how often to automatically refresh backend details
	if v, ok := caller.GetDataset().Options[`refresh_interval`]; ok && v != `` {
		if vInt, err := strconv.ParseInt(v, 10, 32); err == nil {
			self.SchemaRefresh = time.Duration(vInt) * time.Millisecond
		}
	}

	//  how long to wait before a Refresh is considered failsauce
	if v, ok := caller.GetDataset().Options[`refresh_timeout`]; ok && v != `` {
		if vInt, err := strconv.ParseInt(v, 10, 32); err == nil {
			self.SchemaRefreshTimeout = time.Duration(vInt) * time.Millisecond
		} else {
			self.SchemaRefreshTimeout = time.Duration(DEFAULT_SCHEMA_REFRESH_TIMEOUT_MS) * time.Millisecond
		}
	} else {
		self.SchemaRefreshTimeout = time.Duration(DEFAULT_SCHEMA_REFRESH_TIMEOUT_MS) * time.Millisecond
	}

	//  perform the initial population of the schema cache
	if err := caller.Refresh(); err != nil {
		return err
	} else {
		caller.Resume()
	}

	//  start common monitoring
	go Monitor(caller, caller.RefreshInterval())

	return nil
}

func (self *Backend) GetName() string {
	return self.Name
}

func (self *Backend) GetDataset() *dal.Dataset {
	return &self.Dataset
}

func (self *Backend) GetConnectMaxAttempts() int {
	return self.ConnectMaxAttempts
}

func (self *Backend) GetConnectTimeout() time.Duration {
	return self.ConnectTimeout
}

func (self *Backend) RefreshInterval() time.Duration {
	return self.SchemaRefresh
}

func (self *Backend) RefreshTimeout() time.Duration {
	return self.SchemaRefreshTimeout
}

func (self *Backend) RefreshMaxFailures() int {
	return self.SchemaRefreshMaxFail
}

func (self *Backend) IsAvailable() bool {
	return self.Available
}

func (self *Backend) Suspend() {
	if self.Available {
		log.Warningf("Backend %s is unavailable", self.GetName())
	}

	self.Available = false
}

func (self *Backend) Resume() {
	if !self.Available {
		log.Infof("Backend %s is available", self.GetName())
	}

	self.Available = true
}

func (self *Backend) Info() map[string]interface{} {
	return map[string]interface{}{
		`type`: `generic`,
	}
}

func (self *Backend) Disconnect() {
	log.Warningf("Disconnecting backend '%s'", self.GetName())
	self.Suspend()
	self.Connected = false
}

func (self *Backend) ProcessPayload(payloadType PayloadType, payload *dal.RecordSet, req *http.Request) error {
	if payload != nil {
		//  if this is an incoming payload, read/parse/populate now
		if payloadType == RequestPayload {
			if data, err := ioutil.ReadAll(req.Body); err == nil {
				if err := json.Unmarshal(data, &payload); err != nil {
					return err
				}
			} else {
				return err
			}
		}

		if fields := req.URL.Query().Get(`processFields`); fields != `` {
			diffuseFields := strings.Split(fields, `,`)

			//  iterate over the records
			//  we will process each one individually
			for i, record := range payload.Records {
				changed := false

				//  only operate on the named fields
				for _, field := range diffuseFields {
					if value, ok := record[field]; ok {
						input := make(map[string]interface{})

						switch value.(type) {
						case map[string]string:
							for k, v := range value.(map[string]string) {
								input[k] = v
							}
						case map[string]interface{}:
							input = value.(map[string]interface{})
						}

						var errs []error
						var output map[string]interface{}

						switch payloadType {
						case ResponsePayload:
							output, errs = maputil.DiffuseMapTyped(input, `.`, `:`)
						case RequestPayload:
							output, errs = maputil.CoalesceMapTyped(input, `.`, `:`)
						default:
							return fmt.Errorf("Unknown payload type")
						}

						if len(errs) == 0 {
							if len(input) > 0 {
								record[field] = output
								changed = true
							}
						} else {
							for _, err := range errs {
								log.Debugf("Serialization error in diffused field %s: %v", field, err)
							}
						}
					}
				}

				if changed {
					payload.Records[i] = record
				}
			}
		}
	}

	return nil
}

func Monitor(caller IBackend, interval time.Duration) {
	if interval > 0 {
		log.Debugf("Starting %s schema refresh every %s", caller.GetName(), interval)
		schemaRefreshTicker := time.NewTicker(interval)
		didAutosuspend := false
		refreshFailures := 0

		for {
			select {
			case <-schemaRefreshTicker.C:
				log.Debugf("Reloading schema cache for backend '%s' (type: %s)", caller.GetName(), caller.GetDataset().Type)

				reloadDone := make(chan bool)

				go func() {
					if err := caller.Refresh(); err != nil {
						refreshFailures += 1
						log.Debugf("Error reloading schema cache for backend '%s': %v", caller.GetName(), err)

						//  only autosuspend if we're currently available
						if caller.IsAvailable() {
							caller.Suspend()
							didAutosuspend = true
						}

					} else {
						refreshFailures = 0

						//  only autoresume if we autosuspended
						if didAutosuspend {
							log.Debugf("Successfully reloaded schema cache for backend '%s', resuming...", caller.GetName())
							caller.Resume()
							didAutosuspend = false
						}
					}
					reloadDone <- true
				}()

				select {
				case <-reloadDone:

				case <-time.After(caller.RefreshTimeout()):
					refreshFailures += 1
					log.Debugf("Timeout waiting for schema cache to reload for backend '%s', suspending...", caller.GetName())

					if caller.IsAvailable() {
						caller.Suspend()
						didAutosuspend = true
					}
				}

				if refreshFailures > caller.RefreshMaxFailures() {
					log.Errorf("Backend %s has exceeded %d failed health checks", caller.GetName(), caller.RefreshMaxFailures())
					caller.Disconnect()
					return
				}
			}
		}
	}
}
