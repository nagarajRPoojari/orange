#!/bin/bash

echo "creating a schema"
curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "create document test {\"name\":\"STRING\"}"}'

echo "Inserting documents from ID 1 to 20..."

for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8000/ \
       -H "Content-Type: application/json" \
       -d "{\"query\": \"insert value into test  {\\\"_ID\\\": $i, \\\"name\\\": \\\"hello-$i\\\"}\"}"
done

echo "Done."

echo "search a sample doc with id = 13"
curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "select * from test where _ID = 13"}'