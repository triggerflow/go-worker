package main

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"triggerflow/config"
)

func main() {
	configured := false

	log.SetLevel(log.DebugLevel)

	// Set up parameters
	config.UpdateParameters()

	// Load triggerflow config map
	log.Infof("Loading configuration from config file")
	yamlFile, err := ioutil.ReadFile(config.MapFilename)
	if err != nil {
		log.Infof("%v", err)
	} else {
		err = yaml.Unmarshal(yamlFile, &config.Map)
		if err != nil {
			panic(err)
		} else {
			configured = true
		}
	}

	if !configured {
		log.Infof("Loading configuration from env vars")
		err := config.LoadConfigFromEnv()
		if err != nil {
			panic(err)
		} else {
			configured = true
		}
	}

	// Bootstrap worker from ENV var
	if config.BootstrapWorkspace != "" {
		ProcessWorkspace(config.BootstrapWorkspace)
	}

	// Bootstrap worker from command line
	args := os.Args[1:]
	if len(args) == 1 {
		ProcessWorkspace(os.Args[1:][0])
	}

	// Start controller API
	StartController()
}
