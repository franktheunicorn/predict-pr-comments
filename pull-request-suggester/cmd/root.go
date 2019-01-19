// Copyright © 2019 Kris Nova <kris@nivenly.com>
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
	"github.com/holdenk/predict-pr-comments/pull-request-suggester/webhookserver"
	"github.com/kris-nova/logger"
	"github.com/kris-nova/lolgopher"
	"github.com/spf13/cobra"
	"os"
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "pull-request-suggester",
	Short: "HTTP server to serve as a webhook for Pull Request events",
	Long:  `HTTP server to serve as a webhook for Pull Request events`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO handle errors
		webhookserver.Register()
		webhookserver.Serve()
	},
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	logger.FabulousTrueWriter = lol.NewTruecolorLolWriter()
	RootCmd.PersistentFlags().BoolVarP(&logger.Fabulous, "fabulous", "f", false, "Toggle rainbow logs")
	RootCmd.PersistentFlags().BoolVarP(&logger.Color, "color", "X", true, "Toggle colorized logs")
	RootCmd.PersistentFlags().IntVarP(&logger.Level, "verbose", "v", 4, "Log level")

}
