package main

import "net/http"

func runApi(config *NodeConfig) {
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/sync/{id}", &syncHandler{})
	go func() {
		http.ListenAndServe(":7337", mux)
	}()
}

type syncHandler struct{}

func (h *syncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// request should contain
	//  - relative directory path (in URL string)
	//  - JSON representation of peer's directory state
	//  -  { "files": [{ "name", "hash", "modtime", "timestamp" }], "dirs": [] }
	//  -
	p := make([]byte, 128)
	r.Body.Read(p)
	w.Write(p)
}
