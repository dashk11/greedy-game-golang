package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

type UpsertInterface struct {
	Dimension []map[string]string `json:"dim"`
	Metrics   []map[string]string `json:"metrics"`
}

type DataTree struct {
	webRequests int64
	timeSpent   int64
	dimensions  map[string]*DataTree
	metrics     map[string]*DataTree
}

var head *DataTree

func insertDimensions(_dimension string, _metric string, _webRequest int64, _timeSpent int64) {
	fmt.Println(_dimension, _metric)
	if head.dimensions[_dimension] == nil {

		if head.dimensions == nil {
			// First country
			head.dimensions = map[string]*DataTree{_dimension: &DataTree{webRequests: _webRequest, timeSpent: _timeSpent, dimensions: nil, metrics: map[string]*DataTree{_metric: &DataTree{webRequests: _webRequest, timeSpent: _timeSpent, dimensions: nil, metrics: nil}}}}
		} else {
			// New country
			head.dimensions[_dimension] = &DataTree{webRequests: _webRequest, timeSpent: _timeSpent, dimensions: nil, metrics: map[string]*DataTree{_metric: &DataTree{webRequests: _webRequest, timeSpent: _timeSpent, dimensions: nil, metrics: nil}}}
		}

	} else {
		head.dimensions[_dimension].webRequests += _webRequest
		head.dimensions[_dimension].timeSpent += _timeSpent
		if head.dimensions[_dimension].metrics[_metric] == nil {
			// First metric
			head.dimensions[_dimension].metrics = map[string]*DataTree{_metric: &DataTree{webRequests: _webRequest, timeSpent: _timeSpent, dimensions: nil, metrics: nil}}
		} else {
			// New metric
			head.dimensions[_dimension].metrics[_metric].webRequests += _webRequest
			head.dimensions[_dimension].metrics[_metric].timeSpent += _timeSpent
		}
	}
	fmt.Println("_dimension ->", head.dimensions[_dimension])
	fmt.Println("_metric ->", head.dimensions[_dimension].metrics[_metric])
}

func insert(_country string, _device string, _webRequest int64, _timeSpent int64) {
	fmt.Println(_country, _device, _webRequest, _timeSpent)
	if head == nil {
		head = &DataTree{webRequests: _webRequest, timeSpent: _timeSpent, dimensions: nil, metrics: nil}
		insertDimensions(_country, _device, _webRequest, _timeSpent)

	} else {
		head.webRequests += _webRequest
		head.webRequests += _timeSpent
		insertDimensions(_country, _device, _webRequest, _timeSpent)
	}
}

func upsertDimension(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var _request UpsertInterface
	_ = json.NewDecoder(r.Body).Decode(&_request)

	var _webRequest, _timeSpent int64
	var _device, _country string
	_webRequest, _ = strconv.ParseInt(_request.Metrics[0]["val"], 10, 64)
	_timeSpent, _ = strconv.ParseInt(_request.Metrics[1]["val"], 10, 64)
	_device = _request.Dimension[0]["val"]
	_country = _request.Dimension[1]["val"]
	insert(_country, _device, _webRequest, _timeSpent)

	json.NewEncoder(w).Encode(_request)
}

func getDimension(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)

	_dimension := params["country"]
	var response = map[string][]map[string]string{"dim": {{"key": "country", "val": ""}},
		"metrics": {{"key": "webRequest", "val": ""}, {"key": "timeSpent", "val": ""}}}
	response["dim"][0]["val"] = _dimension

	if head == nil || head.dimensions[_dimension] == nil {
		json.NewEncoder(w).Encode("Dimension not found")

	} else {
		response["metrics"][0]["val"] = fmt.Sprintf("%v", head.dimensions[_dimension].webRequests)
		response["metrics"][1]["val"] = fmt.Sprintf("%v", head.dimensions[_dimension].timeSpent)
		json.NewEncoder(w).Encode(response)
	}

}

// Main function
func main() {
	// Init router
	r := mux.NewRouter()

	// Route handles & endpoints
	r.HandleFunc("/v1/insert", upsertDimension).Methods("POST")
	r.HandleFunc("/v1/query/{country}", getDimension).Methods("GET")

	// Start server
	log.Fatal(http.ListenAndServe(":9000", r))
}
