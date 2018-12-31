# data-extract-operator 

A Go library and CLI tool that can optionally be ran as a Kubernetes operator that will pull GitHub pull request data from Big Query and output the result in various ways.

### Building the program

 1) Install [Go](https://golang.org/doc/install)
 2) Edit the `run.sh` file to fit your setup
 3) Run `run.sh`

### Output to file

Run the program with no other verbs.

```bash 
data-extract-operator <flags> -o /path/to/desired/output.csv
```