package pivot

import (
	"io/ioutil"

	"github.com/fatih/structs"
	"github.com/ghodss/yaml"
)

type Configuration struct {
	Backend               string                   `json:"backend"`
	Indexer               string                   `json:"indexer"`
	Autoexpand            bool                     `json:"autoexpand"`
	AutocreateCollections bool                     `json:"autocreate"`
	Environments          map[string]Configuration `json:"environments"`
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

func (self *Configuration) ForEnv(env string) Configuration {
	config := *self
	base := structs.New(&config)

	if envConfig, ok := self.Environments[env]; ok {
		specific := structs.New(envConfig)

		// merge the environment-specific values over top of the general ones
		for _, field := range base.Fields() {
			if field.IsExported() {
				switch field.Name() {
				case `Environment`:
					continue
				default:
					if specificField, ok := specific.FieldOk(field.Name()); ok {
						if !specificField.IsZero() {
							field.Set(specificField.Value())
						}
					}
				}
			}
		}
	}

	return config
}
