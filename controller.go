package main

import (
	"github.com/bitly/go-simplejson"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"net/http"
	"triggerflow/config"
)


func StartController() {
	var (
		err       error
		port      string
	)

	// Create controller service
	router := mux.NewRouter()

	router.HandleFunc("/workspace/{workspace}", startWorker).Methods("POST")

	log.Infof("Starting controller at port %s", config.ControllerPort)
	err = http.ListenAndServe("0.0.0.0:"+port, router)
	if err != nil {
		panic(err)
	}
}

func startWorker(response http.ResponseWriter, request *http.Request) {
	params := mux.Vars(request)
	workspace := params["workspace"]

	json := simplejson.New()
	json.Set("workspace", workspace)

	payload, err := json.MarshalJSON()
	if err != nil {
		log.Error(err)
		response.WriteHeader(500)
		return
	}

	response.Header().Set("Content-Type", "application/json")
	_, err = response.Write(payload)
	if err != nil {
		log.Error(err)
		response.WriteHeader(500)
		return
	}

	log.Infof("Worker for workspace %s started\n", workspace)
	go ProcessWorkspace(workspace)
}
