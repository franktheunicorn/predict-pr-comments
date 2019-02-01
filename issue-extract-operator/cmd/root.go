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
	"io/ioutil"
	"os"

	lol "github.com/kris-nova/lolgopher"

	"github.com/holdenk/predict-pr-comments/issue-extract-operator/queery"
	"github.com/kris-nova/logger"
	"github.com/kubicorn/kubicorn/pkg/local"

	"github.com/spf13/cobra"
)

const (
	outputFileMode = 0644
)

var (
	queeropts  = &queery.Options{}
	outputFile = ""
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "data-extract-operator",
	Short: "Run the data extractor for the PR prediction data",
	Long:  `Use this command to start the operator in the default mode of running a single query as a procedural program.`,

	// Run is the main entry point of the program
	Run: func(cmd *cobra.Command, args []string) {

		ds, err := queery.PullDataSet(queeropts)
		if err != nil {
			logger.Critical("Fatal error during query: %v", err)
			os.Exit(1)
		}
		csvBuffer, err := ds.ToCSV()
		if err != nil {
			logger.Critical("Fatal error during CSV generation: %v", err)
			os.Exit(2)
		}
		err = ioutil.WriteFile(outputFile, csvBuffer.Bytes(), outputFileMode)
		if err != nil {
			logger.Critical("Unable to write file: %v", err)
			logger.Critical("Dumping file:")
			fmt.Println(csvBuffer.String())
			os.Exit(3)
		}
		logger.Always("Wrote CSV to file: %s", outputFile)
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
	logger.FabulousTrueWriter = lol.NewTruecolorLolWriter()
	rootCmd.PersistentFlags().BoolVarP(&logger.Fabulous, "fabulous", "f", false, "Toggle rainbow logs")
	rootCmd.PersistentFlags().BoolVarP(&logger.Color, "color", "X", true, "Toggle colorized logs")
	rootCmd.PersistentFlags().IntVarP(&logger.Level, "verbose", "v", 4, "Log level")
	rootCmd.PersistentFlags().IntVarP(&queeropts.CellLimit, "cell-limit", "l", 100, "The number of cells per set to pull")
	rootCmd.PersistentFlags().StringVarP(&queeropts.DateString, "date", "d", "yesterday", "The datstring to use such as 2018, 20190102, yesterday")
	rootCmd.PersistentFlags().StringVarP(&queeropts.GoogleProject, "google-project", "p", "", "The name of the Google project to use")
	rootCmd.PersistentFlags().StringVarP(&queeropts.PathToAuthFile, "auth-file", "a", fmt.Sprintf("%s/.google/auth.json", local.Home()), "The path to the Google auth file to use")
	rootCmd.Flags().StringVarP(&outputFile, "output-file", "o", fmt.Sprintf("%s/predict-github-comments.csv", local.Home()), "The path to the Google auth file to use")
	rootCmd.AddCommand(cloudCmd)
	rootCmd.AddCommand(operatorCmd)
}
