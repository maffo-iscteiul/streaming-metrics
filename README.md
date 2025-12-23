streaming metrics, a multistore configurable monitor that uses pulsar for streaming

Can be used for alarm, reports, etc...

### UPDATES

go mod tidy
go mod vendor

### BUILD

go build -o streaming-metrics -mod=vendor ./src/main

### Filter funcitons

```json
def log($namespace; $id; $time; $metric): {"namespace": $namespace, "id": $id, "time": $time, "metric": $metric};
```

```json
def filter_error($namespace): error($namespace);
```



### Reminder

1. gojq **test** function is expensive (avoid whenever possible) -> use ctest when possible
2. Do not return a map constant as a metric! - 1 as $v | .... | log(..., {"key": $v})

3. Groups are unqique