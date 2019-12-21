package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Celestial struct {
	Conf Config `yaml:"gravity"`
}

type Config struct {
	ConfVersion    string            `yaml:"version"`
	Address        string            `yaml:"address"`
	Endpoints      []string          `yaml:"db"`
	SEndpoints     []string          `yaml:"sdb"`
	Flusher        string            `yaml:"flusher"`
	InstrumentConf map[string]string `yaml:"instrument"`
}

func ConfigFile(n ...string) (*Config, error) {
	path := "config.yml"
	if len(n) > 0 {
		path = n[0]
	}

	yamlFile, err := ioutil.ReadFile(path)
	check(err)

	var conf Celestial
	err = yaml.Unmarshal(yamlFile, &conf)
	check(err)

	return &conf.Conf, nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
