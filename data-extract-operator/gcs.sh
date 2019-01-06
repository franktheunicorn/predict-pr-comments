#!/bin/bash
set -e
go install .
data-extract-operator cloud \
    --google-project boos-demo-projects-are-rad \
    --cell-limit 10 \
    --auth-file ~/.nova_credentials.json 