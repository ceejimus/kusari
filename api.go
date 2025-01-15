package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

type SyncRequest struct {
	States []SyncFileState `json:"states"`
}

type SyncFileState struct {
	Path      string `json:"path"`
	Hash      string `json:"hash"`
	Size      int64  `json:"size"`
	ModTime   int64  `json:"modtime"`
	Timestamp int64  `json:"timestamp"`
}

func (sfs *SyncFileState) ToFileState() *NodeState {
	return &NodeState{
		Path:    sfs.Path,
		Hash:    &sfs.Hash,
		Size:    sfs.Size,
		ModTime: FromSQLTime(sfs.ModTime),
		// Timestamp: FromSQLTime(sfs.Timestamp),
	}
}

func runApi(config *NodeConfig, db *DB) {
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/sync/{dirPath}", &syncHandler{config, db})
	go func() {
		http.ListenAndServe(":7337", mux)
	}()
}

type syncHandler struct {
	config *NodeConfig
	db     *DB
}

func (h *syncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// request should contain
	//  - relative directory path (in URL string)
	//  - JSON representation of peer's directory state
	//  -  { "files": [{ "name", "hash", "modtime", "timestamp" }], "dirs": [] }
	//  -

	dummyResp := map[string]string{"message": "you are really dumb, for real"}
	dirPath := r.PathValue("dirPath")
	if dirPath == "" {
		logger.Error("what")
		writeError(w, http.StatusBadRequest, dummyResp)
		return
	}

	relDir := filepath.Join(h.config.TopDir, dirPath)
	var managedDir *ManagedDirectory

	for _, d := range h.config.ManagedDirectories {
		logger.Trace(fmt.Sprintf("%q | %+v\n", dirPath, d))
		if d.Path != dirPath {
			continue
		}
		managedDir = &d
	}

	if managedDir == nil {
		writeError(w, http.StatusNotFound, dummyResp)
		return
	}

	logger.Info(fmt.Sprintf("Received SyncRequest for dir: %+v\n", managedDir))

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		// TODO: handle
	}

	var syncRequest SyncRequest
	json.Unmarshal(payload, &syncRequest)
	logger.Info(fmt.Sprintf("Received SyncRequest: \n%+v\n", syncRequest))
	// w.Write([]byte(fmt.Sprintf("syncRequest %+v\n", syncRequest)))

	peerFileStates := make(map[string][]NodeState)

	for _, sfs := range syncRequest.States {
		peerFileState := *sfs.ToFileState()
		fileStates, ok := peerFileStates[peerFileState.Path]
		if !ok {
			fileStates = make([]NodeState, 0)
		}
		peerFileStates[peerFileState.Path] = append(fileStates, peerFileState)
	}

	logger.Info(fmt.Sprintf("FileStates from SyncRequest: \n%+v\n", peerFileStates))

	for filePath, _ := range peerFileStates {
		relPath := filepath.Join(dirPath, filePath)
		fullPath := filepath.Join(h.config.TopDir, relPath)
		logger.Info(fullPath)
		fileinfo, err := os.Lstat(fullPath)
		if err != nil {
			writeError(w, http.StatusBadRequest, map[string]string{"message": fmt.Sprintf("No fileinfo for %q\n%v\n", fullPath, err.Error())})
			return
		}
		fs, err := getNodeState(relDir, fullPath, fileinfo)
		if err != nil {
			writeError(w, http.StatusBadRequest, map[string]string{"message": fmt.Sprintf("Couldn't get host FileState for %q\n%v\n", relPath, err.Error())})
			return
		}
		logger.Info(fmt.Sprintf("Local FileState for %q\n%+v\n", fullPath, fs))
		err = h.db.UpsertFileState(fs)
		if err != nil {
			writeError(w, http.StatusInternalServerError, map[string]string{"message": fmt.Sprintf("Couldn't update FileState for %q\n%v\n", filePath, err.Error())})
			return
		}
	}

	return
}

func writeError(w http.ResponseWriter, statusCode int, resp any) {
	respJson, err := json.Marshal(resp)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to serialize response: %+v\n%v\n", resp, err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")
	w.Write(respJson) // TODO: make sure everything is written
}
