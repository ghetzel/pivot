package pivot

import (
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/util"
	"time"
)

type PivotResponse struct {
	Success     bool                   `json:"success"`
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
	response := PivotResponse{}
	status := util.Status{}

	if err := self.Request(`GET`, `/api/status`, nil, &response, nil); err == nil {
		if err := maputil.StructFromMap(response.Payload, &status); err == nil {
			return status, nil
		} else {
			return status, err
		}
	} else {
		return status, err
	}
}

func (self *Client) Backends() (map[string]backends.Backend, error) {
	response := PivotResponse{}
	backendDefs := make(map[string]backends.Backend)

	if err := self.Request(`GET`, `/api/backends`, nil, &response, nil); err == nil {
		for k, v := range response.Payload {
			switch v.(type) {
			case map[string]interface{}:
				vMap := v.(map[string]interface{})
				backend := backends.Backend{}

				if err := maputil.StructFromMap(vMap, &backend); err == nil {
					backendDefs[k] = backend
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
