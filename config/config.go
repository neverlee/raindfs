package config

import (
	"encoding/json"
	"io/ioutil"
)

var Conf *mainConfig

type mainConfig struct {
	Bind   string `json:"Bind"`
	Rlimit uint64 `json:"Rlimit"`
}

func LoadFile(path string) error {
	def := mainConfig{
		Bind:   "0.0.0.0:8090",
		Rlimit: 8192,
	}

	cfgbyte, rerr := ioutil.ReadFile(path)
	if rerr != nil {
		return rerr
	}

	uerr := json.Unmarshal(cfgbyte, &def)
	if uerr != nil {
		return uerr
	}

	Conf = &def
	return nil
}
