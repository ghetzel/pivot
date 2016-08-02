package pivot

import (
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/util"
	"github.com/ghetzel/pivot/dal"
	"time"
	"fmt"
)

type PivotResponse struct {
	Success     bool                   `json:"success"`
	RespondedAt time.Time              `json:"responded_at"`
	Payload     map[string]interface{} `json:"payload"`
}

type Client struct {
	*util.MultiClient
}

type Backend struct {
	Available            bool          `json:"available"`
	Connected            bool          `json:"connected"`
	ConnectMaxAttempts   int           `json:"max_connection_attempts"`
	ConnectTimeout       time.Duration `json:"connect_timeout"`
	Dataset              dal.Dataset   `json:"configuration"`
	Name                 string        `json:"name"`
	SchemaRefresh        time.Duration `json:"schema_refresh_interval"`
	SchemaRefreshMaxFail int           `json:"schema_refresh_max_failures"`
	SchemaRefreshTimeout time.Duration `json:"schema_refresh_timeout"`
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

func (self *Client) Backends() ([]Backend, error) {
	response := PivotResponse{}
	backendDefs := make([]Backend, 0)

	if err := self.Request(`GET`, `/api/backends`, nil, &response, nil); err == nil {
		for _, v := range response.Payload {
			switch v.(type) {
			case map[string]interface{}:
				vMap := v.(map[string]interface{})
				backend := Backend{}

				fmt.Printf("%+v\n", vMap)

				if err := maputil.StructFromMap(vMap, &backend); err == nil {
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
