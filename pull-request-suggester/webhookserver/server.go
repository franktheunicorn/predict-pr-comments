package webhookserver

import (
	"github.com/gorilla/mux"
	"github.com/kris-nova/logger"
	"net/http"
)

// Server options
// Start server
// Load suggester and configure suggester
// Listen for events
// Append to queue

var (
	router = mux.NewRouter()
)

func Register() error {
	logger.Info("Registering webhook and auth endpoints...")
	router.HandleFunc("/webhook", Webhook)
	router.HandleFunc("/auth", Auth)
	http.Handle("/", router)
	return nil
}

func Serve() error {
	go func() {
		// Kick off Frank
		logger.Info("Starting frank process...")

	}()
	http.ListenAndServe(":80", router)
	return nil
}

func Webhook(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("Success: Webhook"))
	return
}

func Auth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("Success: Auth"))
	return
}
