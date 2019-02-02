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

package cloudstorage

import "cloud.google.com/go/storage"

// Options holds all the possible options we might be interested in using for our sync to cloud storage
type Options struct {

	// GoogleProject is the Google project to use
	GoogleProject string

	// Bucket is the cloud storage bucket to sync to
	Bucket string

	// PathToAuthFile is the path on the local filesystem to find a google authentication file
	PathToAuthFile string

	// SyncWaitSecondAttempts is the amount of attempts to attempt to validate a newly created object has been
	// created via the API. One attempt per second.
	SyncWaitSecondAttempts int
}

type GoogleCloudHandler struct {
	Options *Options
	Client  *storage.Client
}