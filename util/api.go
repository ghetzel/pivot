package util

import (
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

const DEFAULT_MULTICLIENT_HEALTHCHECK_TIMEOUT = (time.Duration(10) * time.Second)

type RequestBodyType int

const (
	BodyRaw RequestBodyType = iota
	BodyJson
	BodyForm
)

type MultiClient struct {
	Addresses          []string
	HealthCheckPath    string
	HealthCheckTimeout time.Duration
	RetryLimit         int
	DefaultBodyType    RequestBodyType
	healthyAddresses   []int
	checkLock          sync.Mutex
	active             bool
}

func NewMultiClient(addresses ...string) *MultiClient {
	return &MultiClient{
		Addresses:          addresses,
		HealthCheckTimeout: DEFAULT_MULTICLIENT_HEALTHCHECK_TIMEOUT,
		RetryLimit:         1,
		DefaultBodyType:    BodyJson,
		active:             true,
	}
}

func (self *MultiClient) SetAddresses(addresses ...string) {
	self.Addresses = addresses
}

func (self *MultiClient) SetHealthCheckPath(path string) {
	self.HealthCheckPath = path
}

func (self *MultiClient) SetHealthCheckTimeout(timeout time.Duration) {
	self.HealthCheckTimeout = timeout
}

func (self *MultiClient) SetRetryLimit(n int) {
	self.RetryLimit = n
}

func (self *MultiClient) SetDefaultBodyType(t RequestBodyType) {
	self.DefaultBodyType = t
}

func (self *MultiClient) Resume() {
	self.active = true
}

func (self *MultiClient) Suspend() {
	self.active = false
	self.CheckAll()
}

func (self *MultiClient) IsActive() bool {
	return self.active
}

func (self *MultiClient) IsHealthy(address string) bool {
	// if the healthcheck path is not set, then simply attempt a TCP socket
	// connection to the address and return whether that was successful or not
	if self.HealthCheckPath == `` {
		if conn, err := net.DialTimeout(`tcp`, address, self.HealthCheckTimeout); err == nil {
			defer conn.Close()
			return true
		}
	}

	return false
}

func (self *MultiClient) GetHealthyAddresses() []string {
	self.checkLock.Lock()
	defer self.checkLock.Unlock()

	addresses := make([]string, len(self.healthyAddresses))

	for i, id := range self.healthyAddresses {
		addresses[i] = self.Addresses[id]
	}

	return addresses
}

func (self *MultiClient) GetRandomHealthyAddress() (string, error) {
	randId := self.healthyAddresses[rand.Intn(len(self.healthyAddresses))]

	if len(self.Addresses) < randId {
		return self.Addresses[randId], nil
	} else {
		return ``, fmt.Errorf("No healthy addresses found")
	}
}

func (self *MultiClient) checkConnect(minSuccessfulAddresses int) error {
	self.checkLock.Lock()
	defer self.checkLock.Unlock()

	if self.IsActive() {
		var successfulChecks int

		self.healthyAddresses = make([]int, 0)

		for i, address := range self.Addresses {
			if self.IsHealthy(address) {
				successfulChecks += 1
				self.healthyAddresses = append(self.healthyAddresses, i)
			}

			if successfulChecks >= minSuccessfulAddresses {
				break
			}
		}

		if successfulChecks < minSuccessfulAddresses {
			return fmt.Errorf("Not enough healthy addresses configured to meet requested minimum: want %d, have %d",
				minSuccessfulAddresses, successfulChecks)
		}

		return nil
	} else {
		self.healthyAddresses = nil
		return fmt.Errorf("Client is not active")
	}
}

func (self *MultiClient) CheckOne() error {
	return self.checkConnect(1)
}

func (self *MultiClient) CheckN(n int) error {
	return self.checkConnect(n)
}

func (self *MultiClient) CheckQuorum() error {
	return self.checkConnect(int(len(self.Addresses)/2) + 1)
}

func (self *MultiClient) CheckAll() error {
	return self.checkConnect(len(self.Addresses))
}

func (self *MultiClient) RequestJSON(method string, path string, payload interface{}) (map[string]interface{}, error) {
	if request, err := NewClientRequest(method, path, payload, self.DefaultBodyType); err == nil {
		output := make(map[string]interface{})
		err := request.PerformJSON(&output)

		return output, err
	} else {
		return nil, err
	}
}

func (self *MultiClient) Request(method string, path string, payload interface{}, output interface{}) error {
	if request, err := NewClientRequest(method, path, payload, self.DefaultBodyType); err == nil {
		_, err := request.Perform(&output, &output)
		return err
	} else {
		return err
	}
}
