package trigger

import (
	"encoding/json"
	"errors"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go"
	"sync"
	"triggerflow/eventsource"
)

type Condition func(context *Context, event cloudevents.Event) (bool, error)

type Action func(context *Context, event cloudevents.Event) error

type Trigger struct {
	TriggerID             string
	UUID                  string
	ConditionFunctionData map[string]string
	Condition             Condition
	ActionFunctionData    map[string]string
	Action                Action
	Context               *Context
	ActivationEvents      []cloudevents.Event
	Transient             bool
	Workspace             string
	Timestamp             string
	Lock                  sync.Mutex
}

type Context struct {
	Workspace           string
	TriggerID           string
	EventSources        map[string]eventsource.EventSource
	EventSink           chan *cloudevents.Event
	Triggers            Map
	TriggerEventMapping ActivationEventMap
	GlobalContext		map[string]string
	RawData             json.RawMessage
	ConditionParsedData interface{}
	ActionParsedData    interface{}
	Modified            bool
}

type Map map[string]*Trigger
type ActivationEventMap map[string]map[string][]*Trigger

type rawJSONTrigger struct {
	ID               string                   `json:"id"`
	UUID             string                   `json:"uuid"`
	Condition        map[string]string        `json:"condition"`
	Action           map[string]string        `json:"action"`
	Context          json.RawMessage          `json:"context"`
	ActivationEvents []map[string]interface{} `json:"activation_events"`
	Transient        bool                     `json:"transient"`
	Workspace        string                   `json:"workspace"`
	Timestamp        string                   `json:"timestamp"`
}

func UnmarshalJSONTrigger(triggerJSON []byte) (*Trigger, error) {
	rawJSONTrigger := rawJSONTrigger{}

	var trigger *Trigger

	if err := json.Unmarshal(triggerJSON, &rawJSONTrigger); err != nil {
		return nil, err
	}

	context := Context{
		TriggerID: rawJSONTrigger.ID,
		RawData:   rawJSONTrigger.Context,
		Modified:  false,
	}

	// Setup activation events
	var activationEvents []cloudevents.Event
	for _, activationEvent := range rawJSONTrigger.ActivationEvents {
		event := cloudevents.NewEvent()
		event.SetSpecVersion(activationEvent["specversion"].(string))
		event.SetSubject(activationEvent["subject"].(string))
		event.SetType(activationEvent["type"].(string))
		event.SetID("actevent")
		event.SetSource("any")
		activationEvents = append(activationEvents, event)
	}

	// Build Trigger struct from parsed JSON trigger
	trigger = &Trigger{
		TriggerID:             rawJSONTrigger.ID,
		UUID:                  rawJSONTrigger.UUID,
		Workspace:             rawJSONTrigger.Workspace,
		ConditionFunctionData: rawJSONTrigger.Condition,
		ActionFunctionData:    rawJSONTrigger.Action,
		Context:               &context,
		ActivationEvents:      activationEvents,
		Transient:             rawJSONTrigger.Transient,
		Timestamp:             rawJSONTrigger.Timestamp,
		Lock:                  sync.Mutex{},
	}

	if condition, ok := Conditions[rawJSONTrigger.Condition["name"]]; ok {
		trigger.Condition = condition
	} else {
		return nil, errors.New(fmt.Sprintf("Condition %s does not exist", rawJSONTrigger.Condition["name"]))
	}

	if action, ok := Actions[rawJSONTrigger.Action["name"]]; ok {
		trigger.Action = action
	} else {
		return nil, errors.New(fmt.Sprintf("Action %s does not exist", rawJSONTrigger.Action["name"]))
	}

	return trigger, nil
}

func MarshalJSONTrigger(trg *Trigger) ([]byte, error) {
	var contextData map[string]interface{}

	actionContext, err := json.Marshal(trg.Context.ActionParsedData)
	if err != nil {
		return nil, err
	}

	conditionContext, err := json.Marshal(trg.Context.ConditionParsedData)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(actionContext, &contextData)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(conditionContext, &contextData)
	if err != nil {
		return nil, err
	}

	JSONtrigger := struct {
		ID               string `json:"id"`
		UUID             string `json:"uuid"`
		Workspace        string `json:"workspace"`
		Transient        bool `json:"transient"`
		Timestamp        string `json:"timestamp"`
		ActivationEvents []cloudevents.Event `json:"activation_events"`
		Condition        map[string]string `json:"condition"`
		Action           map[string]string `json:"action"`
		Context          map[string]interface{} `json:"context"`
	}{
		ID:               trg.TriggerID,
		UUID:             trg.UUID,
		Condition:        trg.ConditionFunctionData,
		Action:           trg.ActionFunctionData,
		Context:          contextData,
		ActivationEvents: trg.ActivationEvents,
		Transient:        trg.Transient,
		Workspace:        trg.Workspace,
		Timestamp:        trg.Timestamp,
	}

	encodedTrigger, err := json.Marshal(JSONtrigger)
	if err != nil {
		return nil, err
	}

	return encodedTrigger, nil
}
