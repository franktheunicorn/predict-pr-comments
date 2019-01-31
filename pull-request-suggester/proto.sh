#!/bin/bash

protoc -I ./ suggester/suggester.proto --go_out=plugins=grpc:suggester