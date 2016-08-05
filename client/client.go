package pivot

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/util"
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger(`pivot`)

type PivotResponse struct {
	Success     bool                   `json:"success"`
	Error       string                 `json:"error"`
	RespondedAt time.Time              `json:"responded_at"`
	Payload     map[string]interface{} `json:"payload"`
}

type Client struct {
	*util.MultiClient
}

func NewClient(address string) *Client {
	client := util.NewMultiClient(address)
	maputil.DefaultStructTag = `json`

	return &Client{
		MultiClient: client,
	}
}

func (self *Client) Status() (util.Status, error) {
	status := util.Status{}

	if response, err := self.Call(`GET`, `/api/status`, nil); err == nil {
		if err := maputil.StructFromMap(response.Payload, &status); err == nil {
			return status, nil
		} else {
			return status, err
		}
	} else {
		return status, err
	}
}

func (self *Client) Backends() ([]*Backend, error) {
	backendDefs := make([]*Backend, 0)

	if response, err := self.Call(`GET`, `/api/backends`, nil); err == nil {
		for _, v := range response.Payload {
			switch v.(type) {
			case map[string]interface{}:
				vMap := v.(map[string]interface{})
				backend := &Backend{
					Client: self,
				}

				if err := maputil.StructFromMap(vMap, &backend.Backend); err == nil {
					backendDefs = append(backendDefs, backend)
				} else {
					return nil, err
				}
			}
		}

		return backendDefs, nil
	} else {
		return nil, err
	}
}

func (self *Client) Call(method string, path string, payload interface{}) (PivotResponse, error) {
	response := PivotResponse{}

	log.Debugf("API: %s %s", method, path)

	if err := self.Request(method, path, payload, &response, &response); err == nil {
		if response.Success {
			return response, nil
		} else {
			if response.Error != `` {
				return response, fmt.Errorf("%s", response.Error)
			} else {
				return response, fmt.Errorf("Request returned an unknown error")
			}
		}
	} else {
		return response, err
	}
}

func (self *Client) GetBackend(name string) (*Backend, error) {
	if backendDefs, err := self.Backends(); err == nil {
		for _, backend := range backendDefs {
			if backend.Name == name {
				return backend, nil
			}
		}
	} else {
		return nil, err
	}

	return nil, fmt.Errorf("Cannot locate backend %q", name)
}
