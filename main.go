package main

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"triggerflow/config"
)

func main() {
	log.SetLevel(log.DebugLevel)

	// Set up parameters
	config.UpdateParameters()

	// Load triggerflow config map
	yamlFile, err := ioutil.ReadFile(config.ConfigMapFilename)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(yamlFile, &config.Map)
	if err != nil {
		panic(err)
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
