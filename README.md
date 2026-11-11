# RPQC

Run all commands from the repository root.

## Single-Machine

### Compile

Requires GNU Make and a C++17-capable G++ compiler. CRoaring is included.

```sh
make -C RDI -j2
```

### Run

The vertex file contains one integer ID per line. Each edge line is
`source predicate_id destination`. The query file contains one numeric-ID
expression per line, using the same predicate IDs as the graph.

Run the queries:

```sh
./RDI/rdi /path/to/vertices.txt /path/to/edges.txt /path/to/queries.txt
```

Results are written to `batch_summary.txt` and `query_logs/` in the current
working directory.

## Distributed

### Compile

Requires GCC, a C++17-capable G++ compiler, POSIX threads, and zlib development
headers. CRoaring is included.

```sh
bash distribute/build.sh
```

### Run

Create two partitions using a vertex-to-partition mapping whose lines are
`vertex partition`:

```sh
./distribute/build/bin/build_partitions_from_v2p \
  -v /path/to/vertices.txt -e /path/to/edges.txt \
  -m /path/to/v2p.txt -o /path/to/partitions -n 2
```

Start each worker in a separate terminal on its assigned host:

```sh
./distribute/build/bin/worker \
  -p 0 -n 2 --port 50051 -d /path/to/partitions/partition_0
```

```sh
./distribute/build/bin/worker \
  -p 1 -n 2 --port 50052 -d /path/to/partitions/partition_1
```

Once both workers are ready, run the coordinator with their reachable IP
addresses. Use `127.0.0.1` for both when running locally:

```sh
./distribute/build/bin/coordinator \
  -w 'WORKER0_IP:50051,WORKER1_IP:50052' \
  -d /path/to/partitions -q '(39){16}' --query-id 1
```

The query uses the graph's numeric predicate IDs. Workers need their partition
files; the coordinator needs the partition metadata and `v2p.txt`. Keep the
workers running and use a new `--query-id` for each subsequent query.
