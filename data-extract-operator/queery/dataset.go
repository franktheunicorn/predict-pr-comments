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
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/kris-nova/logger"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (

	// Query is our main query we can manipulate as needed. This is interpolated later so expect a few erroneous %'s
	// Thanks to Holden Karau for writing the query <3
	RawQuery = `SELECT pull_request_url, 
ANY_VALUE(pull_patch_url) as pull_patch_url, 
ARRAY_AGG(comment_position IGNORE NULLS) as comments_positions, 
ARRAY_AGG(comment_original_position IGNORE NULLS) as comments_original_positions  
FROM (SELECT *, JSON_EXTRACT(payload, '$.action') 
AS action, JSON_EXTRACT(payload, '$.pull_request.url') 
AS pull_request_url, JSON_EXTRACT(payload, '$.pull_request.patch_url') AS pull_patch_url, 
JSON_EXTRACT(payload, '$.comment.original_position') AS comment_original_position, 
JSON_EXTRACT(payload, '$.comment.position') AS comment_position 
FROM ` + "`githubarchive.day.2018*`" + `WHERE type = "PullRequestReviewCommentEvent"
OR type = "PullRequestEvent2") GROUP BY pull_request_url
LIMIT %d`
)

// A representation of the each cell of data back from the above query
// It is important to note that the JSON custom type name MUST match the names of the rows
// from the query above. The code is flexible enough that you can add values above and define
// them below to begin using them in the program downstream.
type Cell struct {
	PullRequestURL      string `json:"pull_request_url"`
	PullRequestPatchURL string `json:"pull_patch_url"`

	// Comments* are actually a set of unsigned integers, but leaving as a raw string type for simplicity
	CommentsPositions                       []string `json:"comments_positions"`
	CommentsOriginalPositions               []string `json:"comments_original_positions"`
	CommentsPositionsSpaceDelimited         string   `json:"comments_positions_space_delimited"`
	CommentsOriginalPositionsSpaceDelimited string   `json:"comments_original_positions_space_delimited"`
}

var (
	// CSVHeader the header to use for the CSV file.
	// Note this will need to be updated if a new value is pulled, but the logic maps on name so we can
	// add new values in arbitrarily without concern for order.
	CSVHeader = [][]string{
		{
			"pull_request_url",
			"pull_patch_url",
			"comments_positions_space_delimited",
			"comments_original_positions_space_delimited",
		},
	}
)

//DataSet is an in memory structure of data we can use for various tasks in our package
type DataSet struct {
	Cells []Cell
}

// PullDataSet is a deterministic operation that will pull a DataSet from Big Query given set of options.
func PullDataSet(opt *Options) (*DataSet, error) {
	ctx := context.Background()
	// TODO @kris-nova use a Kubernetes secret
	logger.Always("Authenticating for google project: %s", opt.GoogleProject)
	client, err := bigquery.NewClient(ctx, opt.GoogleProject, option.WithCredentialsFile(opt.PathToAuthFile))
	if err != nil {
		return nil, err
	}

	// Initalize a new DataSet
	set := DataSet{}

	// Interpolate the query
	interpolatedQuery := fmt.Sprintf(RawQuery, opt.CellLimit)

	// Execute the query
	logger.Always("Loading interpolated query")
	logger.Info("\n\n******************************\n%s\n******************************\n\n", interpolatedQuery)
	q := client.Query(interpolatedQuery)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	defer set.PostProcessFields()

	// This system will iterate through all results and build the in memory representation of the data
	// as defined above.
	for {
		rawCell := make(map[string]bigquery.Value)
		err := it.Next(&rawCell)
		if err == iterator.Done {
			break
		}
		if err != nil {
			// Logic here is to log error and move on
			logger.Warning("Error parsing cell data: %v", err)
			continue
		}
		// Hér eru drekar...
		jsonMemBytes, err := json.Marshal(rawCell)
		if err != nil {
			// Logic here is to log error and move on
			logger.Warning("Error marshalling cell data: %v", err)
			logger.Warning("%+v\n", rawCell)
			continue
		}
		cell := Cell{}
		err = json.Unmarshal(jsonMemBytes, &cell)
		if err != nil {
			// Logic here is to log error and move on
			logger.Warning("Error unmarshalling cell data: %v", err)
			logger.Warning("%+v\n", rawCell)
			continue
		}
		logger.Success("Parsing Pull Request URL: %s", cell.PullRequestURL)
		//logger.Success("%+v\n", cell)
		set.Cells = append(set.Cells, cell)
	}

	return &set, nil
}

// PostProcessFields is just arbitrary logic for sanitizing the data and preparing it for use downstream in the program.
// We literally are just stripping commas and re-writing strings here people, don't freak out.
func (d *DataSet) PostProcessFields() {
	for i, cell := range d.Cells {
		processedCell := cell
		processedCell.PullRequestURL = strings.Replace(cell.PullRequestURL, `"`, "", -1)
		processedCell.PullRequestPatchURL = strings.Replace(cell.PullRequestPatchURL, `"`, "", -1)
		processedCell.CommentsPositionsSpaceDelimited = strings.Join(cell.CommentsPositions, " ")
		processedCell.CommentsOriginalPositionsSpaceDelimited = strings.Join(cell.CommentsOriginalPositions, " ")
		d.Cells[i] = processedCell
	}
}

// ToCSV will convert a DataSet into an RFC-4180 CSV.
// Furthermore this function is a memory bitch and will stick the entire CSV into memory and output the whole thing
// as a buffer. If we start processing large CSVs then we should probably take advantage of the Go standard library
// io.Writer capability and not use a buffer.
func (d *DataSet) ToCSV() (bytes.Buffer, error) {
	buffer := bytes.Buffer{}
	writer := csv.NewWriter(&buffer)
	// Copy our header
	records := CSVHeader

	// One of my more embarrassing hacks. I think this is O(N^3) and that is just hilarious.
	for _, cell := range d.Cells {
		row := make(map[int]string)
		val := reflect.ValueOf(cell)
		for i := 0; i < val.Type().NumField(); i++ {
			tag := val.Type().Field(i).Tag.Get("json")
			for j, headerName := range CSVHeader[0] {
				if headerName == tag {
					row[j] = val.Field(i).String()
				}
			}
		}
		// translate to a slice
		var strs []string
		for _, str := range row {
			strs = append(strs, str)
		}
		records = append(records, strs)
	}
	writer.WriteAll(records)
	logger.Success("Successfully built CSV in memory")
	return buffer, nil
}
