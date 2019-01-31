package suggester

import "github.com/kris-nova/logger"

func StartConcurrentPullRequestProcessor() {
	// error
	// register client
	// pass client to process()
	go process()
}

func process() {

	for {
		nextEvent := Next()
		githubEvent := nextEvent.Event
		logger.Always("Processing event: %s", githubEvent.PullRequest.Title)

		// Hook in here to call the model as we get a Pull Request Event

		// TODO @kris-nova Hook in here
	}
}
