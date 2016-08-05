package pivot

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/pivot/backends"
	"strings"
)

type Backend struct {
	backends.Backend
	Client *Client
}

func (self *Backend) Refresh() error {
	if response, err := self.Client.Call(`GET`, self.GetPath(), nil); err == nil {
		if err := maputil.StructFromMap(response.Payload, &self.Backend); err == nil {
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *Backend) Suspend() error {
	if _, err := self.Client.Call(`PUT`, self.GetPath(`suspend`), nil); err == nil {
		return self.Refresh()
	} else {
		return err
	}
}

func (self *Backend) Resume() error {
	if _, err := self.Client.Call(`PUT`, self.GetPath(`resume`), nil); err == nil {
		return self.Refresh()
	} else {
		return err
	}
}

func (self *Backend) Connect() error {
	if _, err := self.Client.Call(`PUT`, self.GetPath(`connect`), nil); err == nil {
		return self.Refresh()
	} else {
		return err
	}
}

func (self *Backend) Disconnect() error {
	if _, err := self.Client.Call(`PUT`, self.GetPath(`disconnect`), nil); err == nil {
		return self.Refresh()
	} else {
		return err
	}
}

func (self *Backend) GetPath(parts ...string) string {
	if len(parts) == 0 {
		return fmt.Sprintf("/api/backends/%s", self.Name)
	} else {
		return fmt.Sprintf("/api/backends/%s/%s", self.Name, strings.Join(parts, `/`))
	}
}
