package cloudstorage

import (
	"context"
	"fmt"

	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
	"github.com/holdenk/predict-pr-comments/data-extract-operator/queery"
)

// GCSSync will sync the data with Google Cloud Storage based on a given Dataset
func GCSSync(options *queery.Options, dataset *queery.DataSet) error {

	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile(options.PathToAuthFile))
	if err != nil {
		return fmt.Errorf("Unable to initalize new client: %v", err)
	}

	return nil
}
