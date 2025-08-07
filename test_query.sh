#!/bin/bash

echo "creating a schema"
curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "create document item {\"name\":\"STRING\"}"}'

echo "inserting few docs"
curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "insert value into item  {\"_ID\": 1, \"name\":\"hello-1\"}"}'

curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "insert value into item  {\"_ID\": 2, \"name\":\"hello-2\"}"}'

curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "insert value into item  {\"_ID\": 3, \"name\":\"hello-3\"}"}'

echo "search a sample doc with id = 1"
curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "select * from item where _ID = 1"}'