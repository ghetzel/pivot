package pivot

import (
	"fmt"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/backends/dummy"
	"github.com/ghetzel/pivot/backends/elasticsearch"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghodss/yaml"
	"io/ioutil"
)

type Configuration struct {
	Backends map[string]dal.Dataset `json:"backends"`
}

func LoadConfigFile(path string) (Configuration, error) {
	config := Configuration{}

	if data, err := ioutil.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, &config); err != nil {
			return config, err
		}
	} else {
		return config, err
	}

	return config, nil
}

func (self *Configuration) Initialize() error {
	for name, backendConfig := range self.Backends {
		if backend, err := self.initializeBackend(name, backendConfig); err == nil {
			if err := backend.Initialize(); err != nil {
				return fmt.Errorf("Failed to initialize backend '%s': %v", name, err)
			}
		} else {
			return err
		}
	}

	return nil
}

func (self *Configuration) initializeBackend(name string, dataset dal.Dataset) (backends.IBackend, error) {
	var backend backends.IBackend

	switch dataset.Type {
	// case `cassandra`:
	//     backend = cassandra.New(name, dataset)

	case `dummy`:
		backend = dummy.New(name, dataset)

	case `elasticsearch`:
		backend = elasticsearch.New(name, dataset)

	// case `mysql`:
	//     backend = mysql.New(name, dataset)

	// case `postgresql`:
	//     backend = postgresql.New(name, dataset)

	// case `mongodb`:
	//     backend = mongodb.New(name, dataset)

	default:
		return backend, fmt.Errorf("Unrecognized backend type '%s'", dataset.Type)
	}

	if b, ok := Backends[name]; !ok {
		Backends[name] = backend
	} else {
		return backend, fmt.Errorf("A backend named %q is already registered (%T)", name, b)
	}

	return backend, nil
}
