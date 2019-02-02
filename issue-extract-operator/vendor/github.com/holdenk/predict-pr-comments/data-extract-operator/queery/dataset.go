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
	"strings"

	sha2562 "crypto/sha256"

	"reflect"

	"time"

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
 ARRAY_AGG(comment_position) as comments_positions,
 ARRAY_AGG(comment_original_position) as comments_original_positions,
 ARRAY_AGG(comment_commit_id IGNORE NULLS) as comment_commit_ids,
 ARRAY_AGG(comment_file_path IGNORE NULLS) as comment_file_paths  FROM (SELECT *, JSON_EXTRACT(payload, '$.action') AS action,
 JSON_EXTRACT(payload, '$.pull_request.url') AS pull_request_url,
 JSON_EXTRACT(payload, '$.pull_request.patch_url') AS pull_patch_url,
 IFNULL(JSON_EXTRACT(payload, '$.comment.original_position'), "-1") AS comment_original_position,
 IFNULL(JSON_EXTRACT(payload, '$.comment.position'), "-1") AS comment_position,
 JSON_EXTRACT(payload, '$.comment.commit_id') AS comment_commit_id,
 JSON_EXTRACT(payload, '$.comment.path') AS comment_file_path FROM ` + "`githubarchive.day.%s*`" +
 `WHERE type = "PullRequestReviewCommentEvent" OR type = "PullRequestEvent2") GROUP BY pull_request_url
%s`
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

	// File names and commit hashes are represented as strings
	CommentFilePaths                       []string `json:"comment_file_paths"`
	CommentsCommitIds                      []string `json:"comment_commit_ids"`
	CommentFilePathsJsonEncoded            string `json:"comment_file_paths_json_encoded"`
	CommentsCommitIdsSpaceDelimited         string `json:"comment_commit_ids_space_delimited"`

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
			"comment_file_paths_json_encoded",
			"comment_commit_ids_space_delimited",
		},
	}
)

//DataSet is an in memory structure of data we can use for various tasks in our package
type DataSet struct {
	Cells      []Cell
	RawQuery   string
	RawCSV     bytes.Buffer
	CSVSha256  string
	DateString string
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

	// Interpolate the query
	// 1 string data such as (2018) or (20190106)
	// 2 string limit clause if applicable
	var interpolatedQuery string
	if opt.DateString == "yesterday" {
		yesterday := strings.Replace(
			strings.Split(
				time.Now().AddDate(
					0, 0, -1).Format(
					"2006-01-02 15:04:05"), " ")[0], "-", "", -1)
		opt.DateString = yesterday
	}
	if opt.CellLimit == -1 {
		interpolatedQuery = fmt.Sprintf(RawQuery, opt.DateString, "")
	} else {
		interpolatedQuery = fmt.Sprintf(RawQuery, opt.DateString, fmt.Sprintf("LIMIT %d", opt.CellLimit))
	}

	// Initalize a new DataSet
	set := DataSet{
		RawQuery:   interpolatedQuery,
		DateString: opt.DateString,
	}

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
		//logger.Always("%+v\n", cell)
		//logger.Success("Parsing Pull Request URL: %s", cell.PullRequestURL)
		//logger.Success("%+v\n", cell)
		set.Cells = append(set.Cells, cell)
	}
	logger.Always("Data set created in memory")
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
		processedCell.CommentsCommitIdsSpaceDelimited = strings.Join(cell.CommentsCommitIds, " ")
		patchesJson, _ := json.Marshal(cell.CommentFilePaths)
		processedCell.CommentFilePathsJsonEncoded = string(patchesJson)
		d.Cells[i] = processedCell
	}
}

// ToCSV will convert a DataSet into an RFC-4180 CSV.
// Furthermore this function is a memory hog and will stick the entire CSV into memory and output the whole thing
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
					//fmt.Printf("Header Name (%s)\n Tag Name (%s)\n", headerName, tag)
					//fmt.Printf("Field Value (%s)\n\n", val.Field(i).String())
					row[j] = val.Field(i).String()
				}
			}
		}
		var strs []string
		// So basically go randomizes hashmaps to prevent people from doing exactly what I just did
		// which I guess is good, but also really annoying right now
		// Anyway I wrote a dynamic for loop to help rebuild our list of strings, this time in consistent order.
		//
		// ----------------------------
		// Don't ever do this
		// for i, str := range row {
		//   strs = append(strs, str)
		// }
		// ----------------------------
		rLen := len(row)
		for n := 0; n < rLen; n++ {
			strs = append(strs, row[n])
		}
		records = append(records, strs)
	}
	writer.WriteAll(records)
	logger.Success("Successfully built CSV in memory")
	d.RawCSV = buffer

	// Grab a hash of the file for naming later
	sha256 := fmt.Sprintf("%x", sha2562.Sum256(buffer.Bytes()))
	d.CSVSha256 = sha256
	logger.Info("CSV File SHA256 [%s]", sha256)
	return buffer, nil
}
