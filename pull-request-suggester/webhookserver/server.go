package webhookserver

import (
	"encoding/json"
	"fmt"
	"github.com/google/go-github/github"
	"github.com/holdenk/predict-pr-comments/pull-request-suggester/processor"
	"io/ioutil"
	"net/http"
	"strings"

	"bytes"

	"github.com/gorilla/mux"
	"github.com/kris-nova/logger"
)

// Server options
// Start server
// Load processor and configure processor
// Listen for events
// Append to queue

var (
	router = mux.NewRouter()
)

func Register() error {
	logger.Info("Registering webhook and auth endpoints...")
	router.HandleFunc("/webhook", Webhook)
	//router.HandleFunc("/auth", Auth)
	//http.Handle("/", router)
	return nil
}

func Serve(grpcHost, grpcPort string) error {
	go func() {
		// Kick off Frank
		logger.Info("Starting frank process...")

		processor.StartConcurrentProcessorClient(&processor.ClientOptions{
			Hostname: grpcHost,
			Port:     grpcPort,
		})
	}()
	http.ListenAndServe(":80", router)
	return nil
}

func Webhook(w http.ResponseWriter, r *http.Request) {

	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(fmt.Sprintf("500 Server Error: %v", err)))
		return
	}
	event := github.PullRequestEvent{}
	err = json.Unmarshal(bytes, &event)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(fmt.Sprintf("500 Server Error: %v", err)))
		return
	}

	processor.RegisterRequest(r, &event)
	w.WriteHeader(200)
	w.Write([]byte("200 Great Success!"))

	return
}

//func Auth(w http.ResponseWriter, r *http.Request) {
//	fmt.Println(formatRequest(r))
//	w.WriteHeader(200)
//	w.Write([]byte("Success: Auth"))
//	return
//}

func formatRequest(r *http.Request) string {
	// Create return string
	var request []string
	// Add the request string
	url := fmt.Sprintf("%v %v %v", r.Method, r.URL, r.Proto)
	request = append(request, url)
	// Add the host
	request = append(request, fmt.Sprintf("Host: %v", r.Host))
	// Loop through headers
	for name, headers := range r.Header {
		name = strings.ToLower(name)
		for _, h := range headers {
			request = append(request, fmt.Sprintf("%v: %v", name, h))
		}
	}

	bodyBytes, _ := ioutil.ReadAll(r.Body)
	bodyStr := string(bodyBytes)
	prettyString := prettyPrint(bodyStr)
	request = append(request, "")
	request = append(request, prettyString)
	// Return the request as a string
	return strings.Join(request, "\n")
}

func prettyPrint(input interface{}) string {
	output := &bytes.Buffer{}
	if err := json.NewEncoder(output).Encode(input); err != nil {
		logger.Warning("Error building encoder: %v", err)
		return ""
	}
	formatted := &bytes.Buffer{}
	if err := json.Indent(formatted, output.Bytes(), "", "  "); err != nil {
		logger.Warning("Error indenting: %v", err)
		return ""
	}
	return string(formatted.Bytes())
}
