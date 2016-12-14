package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

type SwitchServer struct {
	masterNode string
}

func NewSwitchServer(addr string, masters string, r *mux.Router) *SwitchServer {
	ss := &SwitchServer{
		masterNode: masters,
	}

	r.HandleFunc("/test", ss.testHandler)

	return ss
}

func (v *SwitchServer) testHandler(w http.ResponseWriter, r *http.Request) {
	writeJsonQuiet(w, r, http.StatusOK, "test")
}
