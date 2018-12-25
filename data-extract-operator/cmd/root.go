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
	"github.com/holdenk/predict-pr-comments/data-extract-operator/queery"
	"github.com/kris-nova/logger"
	"os"

	"github.com/spf13/cobra"
)

var queeropts = &queery.Options{}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "data-extract-operator",
	Short: "Run the data extractor for the PR prediction data",
	Long: `This scrapes Big Query for the necessary data to build the machine learning project.`,

	Run: func(cmd *cobra.Command, args []string) {
		ds, err := queery.PullDataSet(queeropts)
		if err != nil {
			logger.Critical("Fatal error: %v", err)
		}
		// TODO (@kris-nova) logic to handle the dataset and write it wherever Holden needs it
		ds.ToCSV()
	},
}



// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
 	// commands
}


