package dal

import (
	"github.com/ghetzel/go-stockutil/stringutil"
	"net/url"
	"strings"
	"os/user"
	"path/filepath"
	"os"
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
		if err := prepareURI(uri); err == nil {
			return ConnectionString{
				URI: uri,
			}, nil
		}else{
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
			URI: uri,
		}, nil
	}else{
		return ConnectionString{}, err
	}
}

func prepareURI(uri *url.URL) error {
	if strings.HasPrefix(uri.Path, `/.`) {
		if cwd, err := os.Getwd(); err == nil {
			if abs, err := filepath.Abs(cwd); err == nil {
				uri.Path = strings.Replace(uri.Path, `/.`, abs, 1)
			}else{
				return err
			}
		}else{
			return err
		}
	}else if strings.HasPrefix(uri.Path, `/~`) {
		if usr, err := user.Current(); err == nil {
			uri.Path = strings.Replace(uri.Path, `/~`, usr.HomeDir, 1)
		}else{
			return err
		}
	}

	return nil
}
