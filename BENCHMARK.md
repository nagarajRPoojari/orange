
Following benchmark was tested on `Apple M3 Pro` with [config.yaml](https://github.com/nagarajRPoojari/orange/tree/main/config.toml) for both storage engine & standalone db instance.
```
Name                             |TotalOps  |Payload  |MB/s    |Ops/s        |Avg Lat(micro sec)  |Time(s)  |GOOS    |ARCH   |CPUs  |GoVersion
orange/read                      |60379     |12       |0.58    |50724.17     |19.71               |1.19     |darwin  |arm64  |12    |go1.24.5
orange/write with jumbo payload  |22309     |10240    |185.31  |18976.11     |52.70               |1.18     |darwin  |arm64  |12    |go1.24.5
orange/write                     |58263     |12       |0.56    |48968.96     |20.42               |1.19     |darwin  |arm64  |12    |go1.24.5
parrot/read                      |11369257  |16       |100.66  |6615399.50   |0.15                |1.72     |darwin  |arm64  |12    |go1.24.5
parrot/write with WAL            |2194867   |16       |27.81   |1849971.95   |0.54                |1.19     |darwin  |arm64  |12    |go1.24.5
parrot/write without WAL         |16557213  |16       |180.96  |11889959.16  |0.08                |1.39     |darwin  |arm64  |12    |go1.24.5
```

1. all `write` does random key insertion with very less likely overriding existing keys.
2. all  `reads` are kept random as sequential reads might not be robust enough because of caching advantage which is less likely to happen in real world.
3. `parrot` provides very high read throughput of `~6.6M`, write throughput of `~1.8M` with WAL & `~11.8M` without WAL.
4. significant decrease in performance is visible in `orange ` because of added `query-parser` & `network` layer.

> [!NOTE]  
> orange is a lightweight, fast, distributed noSQL db.
> It draws inspiration from systems like Cassandra, MongoDB, LevelDB, RocksDB, Pebble, CockroachDB, and many others.

### Run benchmark
```
# benchmark parrot
make benchmark-parrot
```
```
# benchmark orange standalone instance
orange server --port 8000

make benchmark-orange
```

#### view report
```
go run main.go report
```
> [!WARNING]  
> report generated in benchmark directory might be corrupted json file, if you see nothing after `go run main.go report`, please fix corrupted json before trying again
