package cloudstorage

import (
	"context"
	"fmt"

	"strings"

	"sync"

	"time"

	"bytes"

	"cloud.google.com/go/storage"
	"github.com/kris-nova/logger"
	"google.golang.org/api/option"
)

var WriterMutex sync.Mutex

func NewGoogleCloudHandler(options *Options) (*GoogleCloudHandler, error) {
	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile(options.PathToAuthFile))
	if err != nil {
		return nil, fmt.Errorf("Unable to initalize new client: %v", err)
	}
	handler := &GoogleCloudHandler{
		Client:  client,
		Options: options,
	}
	return handler, nil
}

// ObjectExists can be used to see if an existing object already exists
func (gc *GoogleCloudHandler) ObjectExists(objectName string) (bool, error) {
	client := gc.Client
	bucketHandler := client.Bucket(gc.Options.Bucket)
	objects := bucketHandler.Objects(context.TODO(), &storage.Query{Prefix: objectName})
	object, err := objects.Next()
	if err != nil {
		return false, nil
	}
	if object == nil {
		return false, nil
	}
	return true, nil
}

// MutableSyncBytes will sync an object based on a given buffer
func (gc *GoogleCloudHandler) MutableSyncBytes(buffer bytes.Buffer, objectname string) error {
	client := gc.Client
	options := gc.Options
	bucketHandler := client.Bucket(options.Bucket)
	logger.Always("Syncing to bucket: %s", options.Bucket)

	// Now that the bucket has been ensured let's sync the buffer
	objectHandler := bucketHandler.Object(objectname)
	writer := objectHandler.NewWriter(context.TODO())

	// Write to Google Cloud Storage writer with a mutex
	logger.Always("Begin Atomic CSV Sync...")
	WriterMutex.Lock()
	writer.Write(buffer.Bytes())
	writer.Close()
	// Grab a time stamp after we close the network connection
	rfc3339time := time.Now().Format(time.RFC3339)
	WriterMutex.Unlock()

	// attrs is hand for outputting meta information about the newly synced object
	validated := false
	var attrs *storage.ObjectAttrs
	for i := options.SyncWaitSecondAttempts; i >= 0; i-- {
		_, err := objectHandler.Attrs(context.TODO())
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
		// Atomicity is safe here
		_, err := objectHandler.Update(context.TODO(), storage.ObjectAttrsToUpdate{
			Metadata: map[string]string{
				"Timestamp RFC3339": rfc3339time,
				"Public":            "True",
			},
			// Grant read only to the public
			ACL: []storage.ACLRule{
				{
					Entity: storage.AllUsers,
					Role:   storage.RoleReader,
				},
			},
		})
		if err != nil {
			logger.Warning("Unable to update meta information on object: %v", err)
		}

		logger.Always("Successful atomic transaction!")
		logger.Always("Filename: %s", objectname)
		logger.Always("Media Link: %s", attrs.MediaLink)
		logger.Always("Bucket: %s", options.Bucket)
		return nil
	}
	logger.Critical("Unable to validate eventually consistent object after %d attempts", options.SyncWaitSecondAttempts)
	logger.Critical("Trying to recover...")
	err := objectHandler.Delete(context.TODO())
	if err != nil {
		logger.Critical("Unable to recover: %v", err)
		return fmt.Errorf("Uncrecoverable error cleaning up object: %v", err)
	}
	logger.Success("Recovered object %s", objectname)
	return nil
}

// EnsureBucket will ensure a given bucket exists in an account
func (gc *GoogleCloudHandler) EnsureBucket() error {
	client := gc.Client
	bucketName := gc.Options.Bucket
	projectID := gc.Options.GoogleProject
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
		//logger.Info("Checking bucket: %s", bucket.Name)
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