package web
//
//import (
//	"fmt"
//	"gopkg.in/yaml.v2"
//	"io/ioutil"
//	"net/http"
//	"os"
//	"triggerflow"
//
//	"github.com/gorilla/mux"
//	log "github.com/sirupsen/logrus"
//	"triggerflow/definitions"
//)
//
//
//func StartController() {
//	var (
//		err       error
//		port      string
//	)
//
//	// Load triggerflow config map
//	yamlFile, err := ioutil.ReadFile(definitions.ConfigMapFilename)
//	if err != nil {
//		panic(err)
//	}
//
//	err = yaml.Unmarshal(yamlFile, &definitions.ConfigMap)
//	if err != nil {
//		panic(err)
//	}
//
//	// Create controller service
//	router := mux.NewRouter()
//
//	if value, ok := os.LookupEnv("PORT"); ok {
//		port = value
//	} else {
//		port = "5000"
//	}
//
//	router.HandleFunc("/workspace/{workspace}", startWorker).Methods("POST")
//
//	log.Infof("Starting controller at port %s", port)
//	err = http.ListenAndServe("0.0.0.0:"+port, router)
//	if err != nil {
//		panic(err)
//	}
//}
//
//func startWorker(response http.ResponseWriter, request *http.Request) {
//	params := mux.Vars(request)
//	workspace := params["workspace"]
//	_, err := fmt.Fprintf(response, "Worker for workspace %s started\n", workspace)
//	if err != nil {
//		panic(err)
//	}
//	log.Infof("Worker for workspace %s started\n", workspace)
//	go main.ProcessWorkspace(workspace)
//}
