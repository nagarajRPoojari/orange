# orange


> [!NOTE]  
> orange is a lightweight, fast, distributed noSQL db.
> It draws inspiration from systems like Cassandra, MongoDB, LevelDB, RocksDB, Pebble, CockroachDB, and many others.
> The primary goal of this project is to explore and learn the vast and evolving concepts of distributed storage systems.

## Benchmark
To see how `orange` performs checkout [benchmark](https://github.com/nagarajRPoojari/orange/blob/main/BENCHMARK.md)

## Load test your cluster with k6
```
k6 run k6/stress_test.js
k6 run k6/load_test.js
```

### [Orangegate](https://github.com/nagarajRPoojari/orangegate)
##### Stateless proxy behind cluster to route queries
### [Orangectl](https://github.com/nagarajRPoojari/orangectl)
##### k8s operator to manage all cluster resources

## [orangedb](https://github.com/nagarajRPoojari/orange)
- [x] Stores key-doc pair, doc is jsonlike
- [x] Supports Native binary encoding
- [x] Supports strict schema validation
- [x] Primary key based retrieval
- [x] Uses LSM storage engine to provide high write throughput
- [x] WAL for crash recovery
- [x] Background compaction to reduce redundancy
- [x] Standalone deployemnt
- [x] Sharded deployment on k8s cluster
- [x] Sync/Async replication
- [x] Quorum reads for high consistency

# Install

## Standalone
```
go install github.com/nagarajRPoojari/orange@latest
```

## Cluster

> pull all needed images
> > You need to pull all nessessary images mannualy and load to cluster if you are using Kind, minikube etc..
```
docker pull np137270/orange
docker pull np137270/orangectl
docker pull np137270/gateway
docker pull curlimages/curl #Optional: to run e2e tests
```

> deploy orangedb reconciler
```
kubectl apply -f https://raw.githubusercontent.com/nagarajRPoojari/orangectl/main/dist/install.yaml
```
> take a look at sample file [orangectl/config/samples/ctl_v1alpha1_orangectl.yaml](https://github.com/nagarajRPoojari/orangectl/tree/main/config/samples/ctl_v1alpha1_orangectl.yaml)

```yaml
apiVersion: ctl.orangectl.orange.db/v1alpha1
kind: OrangeCtl
metadata:
  name: orangectl-sample
  labels:
    app.kubernetes.io/name: orangectl
    app.kubernetes.io/managed-by: kustomize
spec:
  namespace: default
  router:
    name: router
    labels:
      app: orange-router
      tier: control
    image: np137270/gateway:latest
    port: 8000
    config:
      ROUTING_MODE: "hash"
      LOG_LEVEL: "info"

  shard:
    name: shard
    labels:
      app: orange-shard
      tier: data
    image: np137270/orange:latest
    count: 2                # Number of shards
    replicas: 2             # Replicas per shard
    port: 52001
    config:
      STORAGE_PATH: "/app/data"
      CACHE_ENABLED: "true"

```
```
kubectl apply -f orangedb.yaml
```
```
kubectl port-forward svc/router 8000:8000
```
Execute few test queries
```sh
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

echo "search a sample doc with id = 13"
curl -X POST http://localhost:8000/  \
     -H "Content-Type: application/json" \
     -d '{"query": "select * from test where _ID = 13"}'
```
# Play with orangedb
```
go install github.com/nagarajRPoojari/orange@latest
```
> start local server
```
orange server --port 8000 --address localhost
```
> use repl to play with oql
```
orange repl --port  8000 --address localhost
```

