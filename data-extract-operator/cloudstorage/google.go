package cloudstorage

import (
	"context"
	"fmt"

	"strings"

	"sync"

	"time"

	"cloud.google.com/go/storage"
	"github.com/holdenk/predict-pr-comments/data-extract-operator/queery"
	"github.com/kris-nova/logger"
	"google.golang.org/api/option"
)

var WriterMutex sync.Mutex

// GCSSync will safely sync the data with Google Cloud Storage based on a given Dataset and a set of Options
func GCSSync(options *Options, dataset *queery.DataSet) error {
	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile(options.PathToAuthFile))
	if err != nil {
		return fmt.Errorf("Unable to initalize new client: %v", err)
	}
	err = EnsureGoogleCloudStorageBucket(client, options.Bucket, options.GoogleProject)
	if err != nil {
		return err
	}
	bucketHandler := client.Bucket(options.Bucket)
	logger.Always("Syncing to bucket: %s", options.Bucket)

	// Now that the bucket has been ensured let's sync the CSV based on the hash
	csvBuffer, err := dataset.ToCSV()
	if err != nil {
		return fmt.Errorf("Unable to generate CSV: %v", err)
	}
	csvFileName := fmt.Sprintf("%s.csv", dataset.CSVSha256)
	objectHandler := bucketHandler.Object(csvFileName)
	writer := objectHandler.NewWriter(context.TODO())

	// Write to Google Cloud Storage writer with a mutex
	logger.Always("Begin Atomic CSV Sync...")
	WriterMutex.Lock()
	writer.Write(csvBuffer.Bytes())
	writer.Close()
	WriterMutex.Unlock()

	// attrs is hand for outputting meta information about the newly synced object
	validated := false
	var attrs *storage.ObjectAttrs
	for i := options.SyncWaitSecondAttempts; i >= 0; i-- {
		attrs, err = objectHandler.Attrs(context.TODO())
		if err != nil {
			logger.Debug("Failure parsing object attributes: %v", err)
			time.Sleep(time.Second * 1)
			continue
		} else {
			validated = true
			break
		}
	}
	if validated {
		logger.Always("Successful atomic transaction!")
		logger.Always("Filename: %s", csvFileName)
		logger.Always("Media Link: %s", attrs.MediaLink)
		logger.Always("Bucket: %s", options.Bucket)
		return nil
	}
	logger.Critical("Unable to validate eventually consistent object after %d attempts", options.SyncWaitSecondAttempts)
	logger.Critical("Trying to recover...")
	err = objectHandler.Delete(context.TODO())
	if err != nil {
		logger.Critical("Unable to recover: %v", err)
		return fmt.Errorf("Uncrecoverable error cleaning up object: %v", err)
	}
	logger.Success("Recovered object %s", csvFileName)
	return nil
}

// EnsureGoogleCloudStorageBucket will ensure the bucket exists, and ensure it with versioning in case we fuck
// up our data somehow. So just build a tiny ensure* system to ensure the bucket based on what is already there
// OMG so cloud native
func EnsureGoogleCloudStorageBucket(client *storage.Client, bucketName, projectID string) error {
	bucketHandler := client.Bucket(bucketName)
	found := false

	buckets := client.Buckets(context.Background(), projectID)
	for {
		bucket, err := buckets.Next()
		if err != nil && strings.Contains(err.Error(), "no more items in iterator") {
			logger.Info("Finished scanning buckets", err)
			break
		} else if err != nil {
			logger.Warning("Recoverable error scanning buckets: %v", err)
			continue
		}
		if bucket == nil {
			break
		}
		logger.Info("Checking bucket: %s", bucket.Name)
		if bucket.Name == bucketName {
			logger.Success("Found existing bucket: %s", bucketName)
			found = true
			break
		}
	}
	if found != true {
		logger.Info("Bucket not found, creating bucket: %s versioning: true", bucketName)
		err := bucketHandler.Create(context.Background(), projectID, &storage.BucketAttrs{VersioningEnabled: true})
		if err != nil {
			return fmt.Errorf("Error creating bucket: %s", bucketName)
		}
		logger.Success("Created bucket: %s", bucketName)
	}
	return nil
}
