// Copyright © 2018 Kris Nova <kris@nivenly.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"os"

	"github.com/holdenk/predict-pr-comments/issue-extract-operator/cloudstorage"

	"github.com/holdenk/predict-pr-comments/issue-extract-operator/queery"
	"github.com/kris-nova/logger"

	"github.com/spf13/cobra"
)

var (
	csoptions = &cloudstorage.Options{}
	cloud     = ""
)

var cloudCmd = &cobra.Command{
	Use:   "cloud",
	Short: "Push the data to a cloud storage account",
	Long:  `Use this command to start the operator in the Kubernetes mode and push data to a cloud storage account`,

	// Run is the main entry point of the program
	Run: func(cmd *cobra.Command, args []string) {

		// Todo @kris-nova we have option bloat as per usual please do a consolidate
		csoptions.PathToAuthFile = queeropts.PathToAuthFile
		csoptions.GoogleProject = queeropts.GoogleProject

		err := validate()
		if err != nil {
			logger.Critical("Failed validation: %v", err)
			os.Exit(-1)
		}
		logger.Always("Using cloud: %s", cloud)
		ds, err := queery.PullDataSet(queeropts)
		if err != nil {
			logger.Critical("Fatal error during query: %v", err)
			os.Exit(1)
		}

		switch cloud {
		case "gcs":
			handler, err := cloudstorage.NewGoogleCloudHandler(csoptions)
			if err != nil {
				logger.Critical("Unable to create handler to cloud: %v", err)
				os.Exit(98)
			}
			err = handler.EnsureBucket()
			if err != nil {
				logger.Critical("Unable to ensure bucket: %v", err)
				os.Exit(97)
			}
			buffer, err := ds.ToCSV()
			if err != nil {
				logger.Critical("Unable to generate CSV: %v", err)
				os.Exit(96)
			}
			// Note we do not check for existing here as this is the procederual script
			err = handler.MutableSyncBytes(buffer, fmt.Sprintf("issue_urls/%s.csv", queeropts.DateString))
			if err != nil {
				logger.Critical("Unable to sync bytes: %v", err)
				os.Exit(95)
			}
		default:
			logger.Critical("Historical error: invalid cloud: %s", cloud)
			os.Exit(99)
		}
	},
}

func init() {
	cloudCmd.Flags().StringVarP(&cloud, "cloud", "c", "gcs", "The cloud storage account to use. Currently supported [GCS Google Cloud Storage]")
	cloudCmd.Flags().StringVarP(&csoptions.Bucket, "bucket", "b", "tigeys-buckets-are-rad", "The cloud storage account to use. Currently supported [GCS Google Cloud Storage]")
	cloudCmd.Flags().IntVarP(&csoptions.SyncWaitSecondAttempts, "sync-second-attempts", "N", 30, "When writing a new object to the cloud, we validate it's create. This defines how many single second attempts to wait for the eventually consistent API.")
}

func validate() error {
	if cloud != "gcs" {
		return fmt.Errorf("Invalid cloud: %s", cloud)
	}
	return nil
}
