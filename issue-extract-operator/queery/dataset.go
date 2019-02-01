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
	sha2562 "crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"

	"reflect"

	"cloud.google.com/go/bigquery"
	"github.com/kris-nova/logger"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (

	// Query is our main query we can manipulate as needed. This is interpolated later so expect a few erroneous %'s
	// Thanks to Holden Karau for writing the query <3
	RawQuery = `SELECT repo.name, JSON_EXTRACT(payload, '$.issue.url') 
AS url FROM (SELECT *, JSON_EXTRACT(payload, '$.action') AS action FROM ` + "`githubarchive.day.20*`" + ` WHERE type = "IssuesEvent")
WHERE type = "IssuesEvent"  AND action = "\"opened\"" %s`
)

// A representation of the each cell of data back from the above query
// It is important to note that the JSON custom type name MUST match the names of the rows
// from the query above. The code is flexible enough that you can add values above and define
// them below to begin using them in the program downstream.
type Cell struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

var (
	// CSVHeader the header to use for the CSV file.
	// Note this will need to be updated if a new value is pulled, but the logic maps on name so we can
	// add new values in arbitrarily without concern for order.
	CSVHeader = [][]string{
		{
			"name",
			"url",
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
	if opt.CellLimit == -1 {
		interpolatedQuery = fmt.Sprintf(RawQuery, "")
	} else {
		interpolatedQuery = fmt.Sprintf(RawQuery, fmt.Sprintf("LIMIT %d", opt.CellLimit))
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
