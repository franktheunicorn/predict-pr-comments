# data-extract-operator 

A Go library and CLI tool that can optionally be ran as a Kubernetes operator that will pull GitHub pull request data from Big Query and output the result in various ways.

### Building the program

 1) Install [Go](https://golang.org/doc/install)
 
 Compile the binary
 
 ```bash
 make build
 ```
 
 2) Build and push container container
 
 ```bash
 make container push
 ```
 
 3) Run in Kubernetes
 
 ```bash
 make deploy
 ```
 
 Note: you probably have to edit the makefile to fit your local build system!
