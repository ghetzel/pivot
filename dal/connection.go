package dal

import (
	"github.com/ghetzel/go-stockutil/stringutil"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
)

type ConnectionString struct {
	URI     *url.URL
	Options map[string]interface{}
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

func (self *ConnectionString) HasOpt(key string) bool {
	_, ok := self.Options[key]
	return ok
}

func (self *ConnectionString) OptString(key string, fallback string) string {
	if v, ok := self.Options[key]; ok {
		if vConv, err := stringutil.ConvertToString(v); err == nil {
			return vConv
		}
	}

	return fallback
}

func (self *ConnectionString) OptBool(key string, fallback bool) bool {
	if v, ok := self.Options[key]; ok {
		if vConv, err := stringutil.ConvertToBool(v); err == nil {
			return vConv
		}
	}

	return fallback
}

func (self *ConnectionString) OptInt(key string, fallback int64) int64 {
	if v, ok := self.Options[key]; ok {
		if vConv, err := stringutil.ConvertToInteger(v); err == nil {
			return vConv
		}
	}

	return fallback
}

func (self *ConnectionString) OptFloat(key string, fallback float64) float64 {
	if v, ok := self.Options[key]; ok {
		if vConv, err := stringutil.ConvertToFloat(v); err == nil {
			return vConv
		}
	}

	return fallback
}

func (self *ConnectionString) OptTime(key string, fallback time.Time) time.Time {
	if v, ok := self.Options[key]; ok {
		if vConv, err := stringutil.ConvertToTime(v); err == nil {
			return vConv
		}
	}

	return fallback
}

func ParseConnectionString(conn string) (ConnectionString, error) {
	if uri, err := url.Parse(conn); err == nil {
		if err := prepareURI(uri); err == nil {
			return ConnectionString{
				URI:     uri,
				Options: optionsFromURI(uri),
			}, nil
		} else {
			return ConnectionString{}, err
		}
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

	if err := prepareURI(uri); err == nil {
		return ConnectionString{
			URI:     uri,
			Options: optionsFromURI(uri),
		}, nil
	} else {
		return ConnectionString{}, err
	}
}

func prepareURI(uri *url.URL) error {
	if strings.HasPrefix(uri.Path, `/.`) {
		if cwd, err := os.Getwd(); err == nil {
			if abs, err := filepath.Abs(cwd); err == nil {
				uri.Path = strings.Replace(uri.Path, `/.`, abs, 1)
			} else {
				return err
			}
		} else {
			return err
		}
	} else if strings.HasPrefix(uri.Path, `/~`) {
		if usr, err := user.Current(); err == nil {
			uri.Path = strings.Replace(uri.Path, `/~`, usr.HomeDir, 1)
		} else {
			return err
		}
	}

	return nil
}

func optionsFromURI(uri *url.URL) map[string]interface{} {
	rv := make(map[string]interface{})

	for key, values := range uri.Query() {
		if len(values) > 0 {
			if len(values) == 1 {
				rv[key] = stringutil.Autotype(values[0])
			} else {
				vI := make([]interface{}, len(values))

				for i, vv := range values {
					vI[i] = stringutil.Autotype(vv)
				}

				rv[key] = vI
			}
		}
	}

	return rv
}
