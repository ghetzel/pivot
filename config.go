package pivot

import (
	"github.com/ghodss/yaml"
	"io/ioutil"
)

type Configuration struct {
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
	return nil
}
