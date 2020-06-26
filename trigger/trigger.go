package trigger

import (
	"encoding/json"
	cloudevents "github.com/cloudevents/sdk-go"
	"sync"
	"triggerflow/eventsource"
)

type Condition func(context *Context, event cloudevents.Event) bool

type Action func(context *Context, event cloudevents.Event)

type Trigger struct {
	TriggerID             string
	UUID                  string
	ConditionFunctionData map[string]string
	Condition             Condition
	ActionFunctionData    map[string]string
	Action                Action
	Context               *Context
	ContextParser         func(json.RawMessage) interface{}
	ContextParserData     string
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
	EventSink           chan cloudevents.Event
	Triggers            Map
	TriggerEventMapping ActivationEventMap
	Counter             int
	RawData             json.RawMessage
	ParsedData          interface{}
}

type Map map[string]*Trigger
type ActivationEventMap map[string]map[string][]string

type rawJSONTrigger struct {
	ID               string                   `json:"id"`
	UUID             string                   `json:"uuid"`
	Condition        map[string]string        `json:"condition"`
	Action           map[string]string        `json:"action"`
	Context          json.RawMessage          `json:"context"`
	ContextParser    string                   `json:"context_parser"`
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
	}

	// Setup activation events
	var activationEvents []cloudevents.Event
	for _, activationEvent := range rawJSONTrigger.ActivationEvents {
		event := cloudevents.NewEvent()
		event.SetSpecVersion(activationEvent["specversion"].(string))
		event.SetSubject(activationEvent["subject"].(string))
		event.SetType(activationEvent["type"].(string))
		activationEvents = append(activationEvents, event)
	}

	// Build Trigger struct from parsed JSON trigger
	trigger = &Trigger{
		TriggerID:             rawJSONTrigger.ID,
		UUID:                  rawJSONTrigger.UUID,
		Workspace:             rawJSONTrigger.Workspace,
		ConditionFunctionData: rawJSONTrigger.Condition,
		Condition:             Conditions[rawJSONTrigger.Condition["name"]],
		ActionFunctionData:    rawJSONTrigger.Action,
		Action:                Actions[rawJSONTrigger.Action["name"]],
		Context:               &context,
		ContextParser:         ContextParsers[rawJSONTrigger.ContextParser],
		ContextParserData:     rawJSONTrigger.ContextParser,
		ActivationEvents:      activationEvents,
		Transient:             rawJSONTrigger.Transient,
		Timestamp:             rawJSONTrigger.Timestamp,
		Lock:                  sync.Mutex{},
	}

	return trigger, nil
}

func MarshalJSONTrigger(trg *Trigger) ([]byte, error) {

	context, err := json.Marshal(trg.Context.ParsedData)
	if err != nil {
		panic(err)
	}

	condition, err := json.Marshal(trg.ConditionFunctionData)
	if err != nil {
		panic(err)
	}

	action, err := json.Marshal(trg.ActionFunctionData)
	if err != nil {
		panic(err)
	}

	activationEvents := make([][]byte, len(trg.ActivationEvents))

	for i, actEvt := range trg.ActivationEvents {
		JSONEvent, err := actEvt.MarshalJSON()
		if err != nil {
			panic(err)
		}
		activationEvents[i] = JSONEvent
	}

	JSONtrigger := struct {
		ID               string
		UUID             string
		Condition        []byte
		Action           []byte
		Context          []byte
		ContextParser    string
		ActivationEvents [][]byte
		Transient        bool
		Workspace        string
		Timestamp        string
	}{
		ID:               trg.TriggerID,
		UUID:             trg.UUID,
		Condition:        condition,
		Action:           action,
		Context:          context,
		ContextParser:    trg.ContextParserData,
		ActivationEvents: activationEvents,
		Transient:        trg.Transient,
		Workspace:        trg.Workspace,
		Timestamp:        trg.Timestamp,
	}

	encodedTrigger, err := json.Marshal(JSONtrigger)
	if err != nil {
		panic(err)
	}

	return encodedTrigger, nil
}
