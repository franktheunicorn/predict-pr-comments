package main

import (
	"context"
	"github.com/holdenk/predict-pr-comments/pull-request-suggester/processor/suggester"
	"github.com/kris-nova/logger"
	"google.golang.org/grpc"
	"net"
)

func main() {
	logger.Level = 4
	err := run()
	if err != nil {
		logger.Critical("Error: %v", err)
	}

}

func run() error {
	conn, err := net.Listen("tcp", "0.0.0.0:7777")
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	mockServer := &MockServer{}
	logger.Always("Starting server...")
	suggester.RegisterModelRequestServer(server, mockServer)
	return server.Serve(conn)
}

type MockServer struct {
}

func (m *MockServer) GetComment(ctx context.Context, request *suggester.GetCommentRequest) (*suggester.GetCommentResponse, error) {
	logger.Always("%+v", request)
	return &suggester.GetCommentResponse{}, nil
}
