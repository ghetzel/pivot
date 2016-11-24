package dal

import (
	"github.com/ghetzel/go-stockutil/stringutil"
	"net/url"
	"strings"
)

type ConnectionString struct {
	URI *url.URL
}

func (self *ConnectionString) String() string {
	return self.URI.String()
}

func (self *ConnectionString) Scheme() (string, string) {
	parts := strings.SplitN(self.URI.Scheme, `+`, 2)

	if len(parts) == 1 {
		return parts[0], ``
	} else {
		return parts[0], parts[1]
	}
}

func (self *ConnectionString) Backend() string {
	backend, _ := self.Scheme()
	return backend
}

func (self *ConnectionString) Protocol() string {
	_, protocol := self.Scheme()
	return protocol
}

func (self *ConnectionString) Host() string {
	return self.URI.Host
}

func (self *ConnectionString) Dataset() string {
	return self.URI.Path
}

func ParseConnectionString(conn string) (ConnectionString, error) {
	if uri, err := url.Parse(conn); err == nil {
		return ConnectionString{
			URI: uri,
		}, nil
	} else {
		return ConnectionString{}, err
	}
}

func MakeConnectionString(scheme string, host string, dataset string, options map[string]interface{}) (ConnectionString, error) {
	qs := url.Values{}

	for k, v := range options {
		if strvalue, err := stringutil.ToString(v); err == nil {
			values := strings.Split(strvalue, `,`)
			for _, vv := range values {
				qs.Add(k, vv)
			}
		} else {
			return ConnectionString{}, err
		}
	}

	uri := &url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     dataset,
		RawQuery: qs.Encode(),
	}

	return ConnectionString{
		URI: uri,
	}, nil
}
