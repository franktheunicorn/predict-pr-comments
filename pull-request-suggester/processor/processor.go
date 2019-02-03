package processor

import (
	"context"
	"fmt"
	"github.com/google/go-github/github"
	"github.com/holdenk/predict-pr-comments/frank"
	"github.com/holdenk/predict-pr-comments/pull-request-suggester/processor/suggester"
	"github.com/kris-nova/logger"
	"google.golang.org/grpc"
)

type ClientOptions struct {

	// Configuration to create a client to the gRPC server with
	ModelServerHostname string
	ModelServerPort     string

	// GitHub client credentials
	GithubClientUsername string
	GithubClientPassword string
}

// StartConcurrentProcessorClient will run the service that will connect the ML model
// to GitHub. This probably should be pulled out into a better formed system, but munged
// together for now.
func StartConcurrentProcessorClient(opt *ClientOptions) error {

	// ------------------------------------------------------------------------
	// Load our client connections
	//
	//
	auth := github.BasicAuthTransport{
		Username: opt.GithubClientUsername,
		Password: opt.GithubClientPassword,
	}
	logger.Info("Attempting GitHub auth with user %s", opt.GithubClientUsername)
	githubClient := github.NewClient(auth.Client())
	user, _, err := githubClient.Users.Get(context.Background(), "")
	if err != nil {
		return fmt.Errorf("Unable to authenticate with GitHub: %v", err)
	}
	logger.Info("Authenticated as GitHub user: %s", user.GetName())
	var modelServer *grpc.ClientConn
	logger.Info("Attempting connection to gRPC model server %s:%s", opt.ModelServerHostname, opt.ModelServerPort)
	modelServer, err = grpc.Dial(fmt.Sprintf("%s:%s", opt.ModelServerHostname, opt.ModelServerPort), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("Unable to connect to gRPC server: %v", err)
	}
	defer modelServer.Close()
	modelClient := suggester.NewModelRequestClient(modelServer)

	//
	//
	//
	// ------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	// Run concurrently
	//
	// This System will run the basic frank processer service in a concurrent
	// goroutine
	//
	state := modelServer.GetState()
	for {
		logger.Always("Current gRPC state: %s", state.String())
		event := Next()
		logger.Info("Event recieved for PR: %s", *event.Event.PullRequest.Title)
		response, err := modelClient.GetComment(context.TODO(), &suggester.GetCommentRequest{
			PullRequestPatchURL: *event.Event.PullRequest.PatchURL,
			PullRequestURL:      *event.Event.PullRequest.URL,
			RepoName:            *event.Event.Repo.Name,
		})
		logger.Always("%+v")
		if err != nil {
			logger.Warning("Error from gRPC server: %v", err)
			continue
		}
		// Hooray we have a result back from the model
		for _, f := range response.FileNameCommitIDPositions {
			// ------------------------------------------------------------------------
			// This system will actually make the comment in GitHub!
			owner := event.Event.Repo.Owner.Login
			repo := event.Event.Repo.Name
			prNumber := event.Event.PullRequest.Number
			_, _, err := githubClient.PullRequests.CreateComment(context.Background(), *owner, *repo, *prNumber, &github.PullRequestComment{
				Body:     s(frank.GetMessage()),
				Path:     s(f.FileName),
				CommitID: s(f.CommitID),
				Position: i(int(f.Position)),
			})
			if err != nil {
				logger.Warning("Unable to leave comment on PR: %v File: %s CommitID: %s Position: %d with error: %v", event.Event.PullRequest.Title, f.FileName, f.CommitID, f.Position, err)
				continue
			}
			logger.Info("Frank left comment on PR: %v File: %s CommitID: %s Position: %d with error: %v", event.Event.PullRequest.Title, f.FileName, f.CommitID, f.Position, err)
		}
	}

	return nil

}

func s(s string) *string {
	return &s
}

func i(i int) *int {
	return &i
}
