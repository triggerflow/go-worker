package config

import (
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

var (
	SinkMaxSize        = 200000
	ConfigMapFilename  = "config_map.yaml"
	BootstrapWorkspace = ""
	ControllerPort	   = 5000
)

func UpdateParameters() {
	var (
		param string
		value int
		err   error
	)

	param = os.Getenv("TRIGGERFLOW_SINK_MAX_SIZE")
	if param != "" {
		value, err = strconv.Atoi(param)
		if err != nil {
			SinkMaxSize = value
		} else {
			log.Errorf("Could not parse TRIGGERFLOW_SINK_MAX_SIZE parameter from %s", param)
		}
		log.Infof("Set parameter TRIGGERFLOW_SINK_MAX_SIZE to %d", SinkMaxSize)
	}

	param = os.Getenv("TRIGGERFLOW_CONFIG_MAP_FILE")
	if param != "" {
		ConfigMapFilename = param
		log.Infof("Set parameter TRIGGERFLOW_CONFIG_MAP_FILE to %s", ConfigMapFilename)
	}

	param = os.Getenv("TRIGGERFLOW_BOOTSTRAP_WORKSPACE")
	if param != "" {
		BootstrapWorkspace = param
		log.Infof("Set parameter TRIGGERFLOW_BOOTSTRAP_WORKSPACE to %s", BootstrapWorkspace)
	}

	param = os.Getenv("TRIGGERFLOW_CONTROLLER_PORT")
	if param != "" {
		value, err := strconv.Atoi(param)
		if err != nil {
			ControllerPort = value
		} else {
			log.Errorf("Could not parse TRIGGERFLOW_CONTROLLER_PORT parameter from %s", param)
		}
		log.Infof("Set parameter TRIGGERFLOW_CONTROLLER_PORT to %s", ControllerPort)
	}

	log.Debugf("TRIGGERFLOW_SINK_MAX_SIZE=%d", SinkMaxSize)
	log.Debugf("TRIGGERFLOW_CONFIG_MAP_FILE=%s", ConfigMapFilename)
	log.Debugf("TRIGGERFLOW_BOOTSTRAP_WORKSPACE=%s", BootstrapWorkspace)
	log.Debugf("TRIGGERFLOW_CONTROLLER_PORT=%d", ControllerPort)
}