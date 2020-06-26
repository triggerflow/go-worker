package definitions

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

var ConfigMap TriggerflowConfigMap
