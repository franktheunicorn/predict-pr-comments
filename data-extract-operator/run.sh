#!/bin/bash
go install .
data-extract-operator \
    --google-project boos-demo-projects-are-rad \
    --cell-limit 10 \
    --auth-file ~/.nova_credentials.json 