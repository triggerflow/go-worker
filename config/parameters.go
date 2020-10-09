package config

import (
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
)

var (
	SinkMaxSize        = 200000
	MapFilename        = "config_map.yaml"
	BootstrapWorkspace = ""
	ControllerPort     = 5000
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
		MapFilename = param
		log.Infof("Set parameter TRIGGERFLOW_CONFIG_MAP_FILE to %s", MapFilename)
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
	log.Debugf("TRIGGERFLOW_CONFIG_MAP_FILE=%s", MapFilename)
	log.Debugf("TRIGGERFLOW_BOOTSTRAP_WORKSPACE=%s", BootstrapWorkspace)
	log.Debugf("TRIGGERFLOW_CONTROLLER_PORT=%d", ControllerPort)
}

func SetLogLevel() {
	loglevel := os.Getenv("LOG_LEVEL")
	loglevel = strings.ToUpper(loglevel)
	switch loglevel {
	case "TRACE":
		log.SetLevel(log.TraceLevel)
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "WARNING":
		log.SetLevel(log.WarnLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	case "FATAL":
		log.SetLevel(log.FatalLevel)
	case "PANIC":
		log.SetLevel(log.PanicLevel)
	default:
		log.SetLevel(log.PanicLevel)
	}
}
