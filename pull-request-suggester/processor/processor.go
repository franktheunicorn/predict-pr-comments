package processor

import (
	"fmt"
	"github.com/holdenk/predict-pr-comments/pull-request-suggester/processor/suggester"
	"google.golang.org/grpc"
	"log"
)

type ClientOptions struct {
	Hostname string
	Port     string
}

func StartConcurrentProcessorClient(opt *ClientOptions) {

	// Todo pass this in from the CLI

	var conn *grpc.ClientConn
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", opt.Hostname, opt.Port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err)
	}
	defer conn.Close()
	client := suggester.NewModelRequestClient(conn)

	// Run concurrently
	go func() {
		// do shit with client
		fmt.Println(client)
	}()
}

func process() {

}
