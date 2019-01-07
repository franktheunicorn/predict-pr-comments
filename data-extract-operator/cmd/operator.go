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
	"strings"

	"github.com/holdenk/predict-pr-comments/data-extract-operator/cloudstorage"

	"github.com/kris-nova/logger"

	"time"

	"github.com/holdenk/predict-pr-comments/data-extract-operator/queery"
	"github.com/spf13/cobra"
)

var operatorCmd = &cobra.Command{
	Use:   "operator",
	Short: "Run the program as a Kubernetes Operator",
	Long:  `Use this command to start the operator in the Kubernetes operator mode and push data to a cloud storage account consecutively.`,

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

		switch cloud {
		case "gcs":
			cancel := make(chan bool)
			errch := ControlLoop(cancel)
			for {
				logger.Warning("Control loop error: %v", <-errch)
				// Logic for cancel <- true
			}
		default:
			logger.Critical("Historical error: invalid cloud: %s", cloud)
			os.Exit(99)
		}
	},
}

func init() {
	operatorCmd.Flags().StringVarP(&cloud, "cloud", "c", "gcs", "The cloud storage account to use. Currently supported [GCS Google Cloud Storage]")
	operatorCmd.Flags().StringVarP(&csoptions.Bucket, "bucket", "b", "tigeys-buckets-are-rad", "The cloud storage account to use. Currently supported [GCS Google Cloud Storage]")
	operatorCmd.Flags().IntVarP(&csoptions.SyncWaitSecondAttempts, "sync-second-attempts", "N", 30, "When writing a new object to the cloud, we validate it's create. This defines how many single second attempts to wait for the eventually consistent API.")
}

// ControlLoop is a concurrent control loop that will run as a Kubernetes Operator
func ControlLoop(cancel chan bool) chan error {

	errch := make(chan error)
	go func() {

		client, err := cloudstorage.NewGoogleCloudHandler(csoptions)
		if err != nil {
			errch <- fmt.Errorf("Unable to create client: %v", err)
			cancel <- true
		}
		first := true
		for {
			select {
			case <-cancel:
				break
			default:
				// ----------------------------------------------------
				{
					if first != true {
						time.Sleep(time.Minute * 15)
					} else {
						first = false
					}
					// Hacky way of getting yesterday
					yesterday := strings.Replace(
						strings.Split(
							time.Now().AddDate(
								0, 0, 0).Format(
								"2006-01-02 15:04:05"), " ")[0], "-", "", -1)
					objectName := fmt.Sprintf("%s.csv", yesterday)
					exists, err := client.ObjectExists(objectName)
					if err != nil {
						errch <- fmt.Errorf("Error reading object: %v", err)
						continue
					}
					if !exists {
						logger.Always("Writing new dataset. Existing file %s not found.", objectName)
						queeropts.DateString = yesterday
						ds, err := queery.PullDataSet(queeropts)
						if err != nil {
							errch <- fmt.Errorf("Error reading dataset: %v", err)
							continue
						}
						err = client.EnsureBucket()
						if err != nil {
							errch <- fmt.Errorf("Error ensuring bucket: %v", err)
							continue
						}
						buffer, err := ds.ToCSV()
						if err != nil {
							errch <- fmt.Errorf("Error generating CSV: %v", err)
							continue
						}
						err = client.MutableSyncBytes(buffer, objectName)
						if err != nil {
							errch <- fmt.Errorf("Error writing CSV: %v", err)
							continue
						}
					} else {
						logger.Always("File exists %s exists", objectName)
					}

				}
			}
		}
	}()
	return errch

}

//handler, err := cloudstorage.NewGoogleCloudHandler(csoptions)
//if err != nil {
//logger.Critical("Unable to create handler to cloud: %v", err)
//os.Exit(98)
//}
//err = handler.EnsureBucket()
//if err != nil {
//logger.Critical("Unable to ensure bucket: %v", err)
//os.Exit(97)
//}
//buffer, err := ds.ToCSV()
//if err != nil {
//logger.Critical("Unable to generate CSV: %v", err)
//os.Exit(96)
//}
//// Note we do not check for existing here as this is the procederual script
//err = handler.MutableSyncBytes(buffer, fmt.Sprintf("%s.csv", ds.DateString))
//if err != nil {
//logger.Critical("Unable to sync bytes: %v", err)
//os.Exit(95)
//}
