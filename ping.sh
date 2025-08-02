#!/bin/bash

payload='{"query":"create document temp {\"name\": \"STRING\"}"}'

curl -X POST http://localhost:8000/ \
     -H "Content-Type: application/json" \
     -d "$payload"

payload='{"query":"insert value into temp {\"_ID\":1, \"name\": \"nagaraj\"}"}'

curl -X POST http://localhost:8000/ \
     -H "Content-Type: application/json" \
     -d "$payload"

payload='{"query":"select * from temp where _ID = 1"}'

curl -X POST http://localhost:8000/ \
     -H "Content-Type: application/json" \
     -d "$payload"
