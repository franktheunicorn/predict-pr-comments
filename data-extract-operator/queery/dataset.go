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

package queery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (

	// Queery the stub query we can manipulate as needed
	// TODO @kris-nova came we have some fmt.String fun here and clean this up please?
	// TODO @kris-nova we removed the ` from the query around `githubarchive.day.2018` so we need to clean the string up
	Query = `SELECT pull_request_url, 
ANY_VALUE(pull_patch_url) as pull_patch_url, 
ARRAY_AGG(comment_position IGNORE NULLS) as comments_positions, 
ARRAY_AGG(comment_original_position IGNORE NULLS) as comments_original_positions  
FROM (SELECT *, JSON_EXTRACT(payload, '$.action') 
AS action, JSON_EXTRACT(payload, '$.pull_request.url') 
AS pull_request_url, JSON_EXTRACT(payload, '$.pull_request.patch_url') AS pull_patch_url, 
JSON_EXTRACT(payload, '$.comment.original_position') AS comment_original_position, 
JSON_EXTRACT(payload, '$.comment.position') AS comment_position 
FROM 'githubarchive.day.2018*' WHERE type = "PullRequestReviewCommentEvent" 
OR type = "PullRequestEvent2") GROUP BY pull_request_url`

)

//DataSet is an in memory structure of data we can use for various tasks in our package
type DataSet struct {

	// TODO @kris-nova build out an interator here please
}


// PullDataSet is a deterministic operation that will pull a DataSet from Big Query given set of options.
func PullDataSet(opt *Options) (*DataSet, error) {


	ctx := context.Background()
	// TODO @kris-nova use a Kubernetes secret
	client, err := bigquery.NewClient(ctx, googleProject, option.WithCredentialsFile("/Users/nova/.nova_credentials.json"))
	if err != nil {
		return nil, err
	}
	q := client.Query(Query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		fmt.Println(values)
		// TODO @kris-nova let's stick this into a mutable slice in memory and we can process to CSV later
	}

	//return nil

	return nil, nil
}


func (d *DataSet) ToCSV() {
	// TODO @kris-nova make this export a CSV structure
}