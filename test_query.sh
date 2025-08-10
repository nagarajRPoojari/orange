#!/bin/bash

echo "creating a schema"
curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "create document item {\"name\":\"STRING\"}"}'

echo "Inserting documents from ID 1 to 10..."

for i in $(seq 1 2); do
  curl -s -X POST http://localhost:8000/ \
       -H "Content-Type: application/json" \
       -d "{\"query\": \"insert value into item  {\\\"_ID\\\": $i, \\\"name\\\": \\\"hello-$i\\\"}\"}"
done

echo "Done."

echo "search a sample doc with id = 4"
curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "select * from item where _ID = 1"}'