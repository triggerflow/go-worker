package definitions

import (
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

var (
	SinkMaxSize       int    = 200000
	ConfigMapFilename string = "config_map.yaml"
	CheckpointEnabled bool   = false
	CheckpointSteps   uint32 = 0
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
	}
	log.Infof("Set parameter TRIGGERFLOW_SINK_MAX_SIZE to %i", SinkMaxSize)

	param = os.Getenv("TRIGGERFLOW_CONFIG_MAP_FILE")
	if param != "" {
		ConfigMapFilename = param
	}
	log.Infof("Set parameter TRIGGERFLOW_CONFIG_MAP_FILE to %i", ConfigMapFilename)

	param = os.Getenv("TRIGGERFLOW_CHECKPOINT")
	if param != "" {
		value, err := strconv.Atoi(param)
		if err != nil {
			CheckpointSteps = uint32(value)
		} else {
			log.Errorf("Could not parse TRIGGERFLOW_CHECKPOINT parameter from %s", param)
		}
	}
	log.Infof("Set parameter TRIGGERFLOW_CHECKPOINT to %i", CheckpointSteps)
}
