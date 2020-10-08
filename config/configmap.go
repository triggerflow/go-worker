package config

import (
	"errors"
	"os"
	"strconv"
)

type TriggerflowConfigMap struct {
	TriggerflowController struct {
		Endpoint string `yaml:"endpoint"`
		Token    string `yaml:"token"`
	} `yaml:"triggerflow_controller"`
	TriggerStorage struct {
		Backend    string                 `yaml:"backend"`
		Parameters map[string]interface{} `yaml:"parameters"`
	} `yaml:"trigger_storage"`
}

var Map TriggerflowConfigMap

func LoadConfigFromEnv() error {
	if env, ok := os.LookupEnv("TRIGGERFLOW_STORAGE_BACKEND"); ok {
		Map.TriggerStorage.Backend = env
	} else {
		return errors.New("env TRIGGERFLOW_STORAGE_BACKEND not set")
	}

	switch Map.TriggerStorage.Backend {
	case "redis":
		params := make(map[string]interface{})
		if env, ok := os.LookupEnv("REDIS_HOST"); ok {
			params["host"] = env
		} else {
			return errors.New("env REDIS_HOST not set")
		}
		if env, ok := os.LookupEnv("REDIS_PASSWORD"); ok {
			params["password"] = env
		} else {
			params["password"] = ""
		}
		if env, ok := os.LookupEnv("REDIS_PORT"); ok {
			port, err := strconv.Atoi(env)
			if err != nil {
				return err
			}
			params["port"] = port
		} else {
			params["port"] = 6379
		}
		if env, ok := os.LookupEnv("REDIS_DB"); ok {
			db, err := strconv.Atoi(env)
			if err != nil {
				return err
			}
			params["db"] = db
		} else {
			params["db"] = 0
		}
		Map.TriggerStorage.Parameters = params
	}

	return nil
}
