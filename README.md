# orange


> [!NOTE]  
> orange is a lightweight, fast, distributed noSQL db.
> It draws inspiration from systems like Cassandra, MongoDB, LevelDB, RocksDB, Pebble, CockroachDB, and many others
> The primary goal of this project is to explore and learn the vast and evolving concepts of distributed storage systems.

### [Orangegate](https://github.com/nagarajRPoojari/gateway)
##### Proxy behind cluster to manage shards & route queries
### [Orangectl](https://github.com/nagarajRPoojari/orangectl)
##### k8s operator to manager cluster

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
> deploy orangedb reconciler
```
kubectl apply -f https://raw.githubusercontent.com/nagarajRPoojari/orangectl/main/dist/install.yaml
```
> take a look at sample file [orangectl/config/samples/ctl_v1alpha1_orangectl.yaml](https://github.com/nagarajRPoojari/orangectl/config/samples/ctl_v1alpha1_orangectl.yaml)
```
kubectl apply -f orangedb.yaml
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

