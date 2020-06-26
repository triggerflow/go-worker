package main

import (
	"encoding/json"
	"sync/atomic"
	"triggerflow/definitions"
	"triggerflow/eventsource"
	"triggerflow/tirggerstorage"
	"triggerflow/trigger"

	cloudevents "github.com/cloudevents/sdk-go"
	log "github.com/sirupsen/logrus"
)

type Workspace struct {
	WorkspaceName       string
	Triggers            trigger.Map
	TriggerEventMapping trigger.ActivationEventMap
	GlobalContext       map[string]interface{}
	TriggerStorage      tirggerstorage.Storage
	EventSources        map[string]eventsource.EventSource
	EventSink           chan cloudevents.Event
	CheckpointCounter   uint32
}

func ProcessWorkspace(workspaceName string) {
	workspace := Workspace{
		WorkspaceName:       workspaceName,
		Triggers:            make(trigger.Map),
		TriggerEventMapping: make(trigger.ActivationEventMap),
		GlobalContext:       make(map[string]interface{}),
		TriggerStorage:      nil,
		EventSources:        make(map[string]eventsource.EventSource),
		EventSink:           make(chan cloudevents.Event, definitions.SinkMaxSize),
		CheckpointCounter:   0,
	}

	workspace.startTriggerStorage()
	workspace.startEventSources()
	workspace.updateTriggers()

	for event := range workspace.EventSink {
		if matchingTriggers, ok := workspace.TriggerEventMapping[event.Subject()][event.Type()]; ok {
			for _, triggerID := range matchingTriggers {
				go workspace.processTrigger(triggerID, event)
				//workspace.processTrigger(triggerID, event)
			}
		} else {
			log.Infof("Received event with subject <%s> and type <%s> not found in local trigger cache", event.Subject(), event.Type())
			workspace.updateTriggers()
			workspace.EventSink <- event
		}
	}
}

func (workspace *Workspace) processTrigger(triggerID string, event cloudevents.Event) {
	//log.Debugf("Processing trigger <%s>", triggerID)
	trg := workspace.Triggers[triggerID]
	trg.Lock.Lock()
	condition := trg.Condition(trg.Context, event)
	if condition {
		trg.Action(trg.Context, event)
		//log.Infof("Trigger %s action fired", triggerID)
	}
	trg.Lock.Unlock()

	if condition && definitions.CheckpointEnabled {
		cnt := atomic.AddUint32(&workspace.CheckpointCounter, 1)
		if cnt >= definitions.CheckpointSteps {
			atomic.StoreUint32(&workspace.CheckpointCounter,0)
			workspace.checkpointTriggers()
		}
	}
}

func (workspace *Workspace) updateTriggers() {
	log.Infof("Updating trigger cache...")
	allTriggers := workspace.TriggerStorage.Get((*workspace).WorkspaceName, "triggers")

	for triggerID, triggerJSON := range allTriggers {
		if _, ok := workspace.Triggers[triggerID]; !ok {

			newTrigger, err := trigger.UnmarshalJSONTrigger([]byte(triggerJSON))
			if err != nil {
				log.Warnf("Encountered error during JSON Trigger unmarshal: %s", err)
				continue
			}

			workspace.contextualizeTrigger(newTrigger)
			workspace.Triggers[newTrigger.TriggerID] = newTrigger

			for _, actEvt := range newTrigger.ActivationEvents {
				if _, ok := workspace.TriggerEventMapping[actEvt.Subject()]; !ok {
					workspace.TriggerEventMapping[actEvt.Subject()] = make(map[string][]string)
				}

				if _, ok := workspace.TriggerEventMapping[actEvt.Subject()][actEvt.Type()]; !ok {
					workspace.TriggerEventMapping[actEvt.Subject()][actEvt.Type()] = make([]string, 0)
				}

				trgIDs := workspace.TriggerEventMapping[actEvt.Subject()][actEvt.Type()]
				workspace.TriggerEventMapping[actEvt.Subject()][actEvt.Type()] = append(trgIDs, newTrigger.TriggerID)
			}

			log.Debugf("Added new trigger to cache: <%s> <%s>", newTrigger.TriggerID, newTrigger.UUID)
		}
	}

	log.Infof("Triggers updated -- %i triggers in local cache", len(workspace.Triggers))
}

func (workspace *Workspace) contextualizeTrigger(trigger *trigger.Trigger) {
	(*trigger).Context.EventSink = workspace.EventSink
	(*trigger).Context.EventSources = workspace.EventSources
	(*trigger).Context.Triggers = workspace.Triggers
	(*trigger).Context.TriggerEventMapping = workspace.TriggerEventMapping
	(*trigger).Context.ParsedData = (*trigger).ContextParser((*trigger).Context.RawData)
}

func (workspace *Workspace) startTriggerStorage() {
	TriggerStorage := tirggerstorage.BackendConstructors[definitions.ConfigMap.TriggerStorage.Backend]
	workspace.TriggerStorage = TriggerStorage(definitions.ConfigMap.TriggerStorage.Parameters)
}

func (workspace *Workspace) startEventSources() {
	eventSources := workspace.TriggerStorage.Get(workspace.WorkspaceName, "event_sources")

	for _, evtSourceJSON := range eventSources {
		eventSourceMeta := struct {
			Class      string
			Name       string
			Parameters json.RawMessage
		}{}

		if err := json.Unmarshal([]byte(evtSourceJSON), &eventSourceMeta); err != nil {
			panic(err)
		}

		// Instantiate EventSource and start consuming events
		EventSource := eventsource.Constructors[eventSourceMeta.Class]
		workspace.EventSources[eventSourceMeta.Name] = EventSource(workspace.WorkspaceName, workspace.EventSink, eventSourceMeta.Parameters)
		go workspace.EventSources[eventSourceMeta.Name].StartConsuming()
	}
}

func (workspace *Workspace) checkpointTriggers() {
	log.Infof("Checkpoint")
	// Pause all event sources so no new events go into the processing pipeline
	for _, eventSource := range workspace.EventSources {
		eventSource.Pause()
	}

	encodedTriggers := make(map[string][]byte, len(workspace.Triggers))
	for _, trg := range workspace.Triggers {
		trg.Lock.Lock()
		encodedTrigger, err := trigger.MarshalJSONTrigger(trg)
		trg.Lock.Unlock()
		if err != nil {
			panic(err)
		}
		encodedTriggers[trg.TriggerID] = encodedTrigger
	}

	// Commit cached records and resume consuming
	for _, eventSource := range workspace.EventSources {
		eventSource.CommitEvents()
		eventSource.Resume()
	}

	// Update triggers in persistent storage
	for triggerID, encodedTrigger := range encodedTriggers {
		triggerID := triggerID; encodedTrigger := encodedTrigger
		go func(string, []byte) {
			workspace.TriggerStorage.Put(workspace.WorkspaceName, "triggers", triggerID, encodedTrigger)
		}(triggerID, encodedTrigger)
	}
}
