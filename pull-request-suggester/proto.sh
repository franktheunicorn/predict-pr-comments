#!/bin/bash

protoc -I ./ suggestor/suggester.proto --go_out=plugins=grpc:processor