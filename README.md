# RPQC

RPQC evaluates regular path queries with exact, bounded, lower-unbounded, and
nested counting operators. Results are distinct endpoint pairs over walks;
vertices and edges may be revisited.

## Repository Layout

| Directory | Contents |
|---|---|
| `RDI/` | Single-machine GCNP implementation, build files, and correctness tests |
| `distribute/` | Distributed implementation, RPC support, and partition tools |
| `query/` | Final core and robustness query expressions, predicate map, and verifier |
| `dataset/` | Dataset download sources |

## Build the Single-Machine Implementation

Requires GNU Make and a C++17-capable G++ compiler. CRoaring headers and the
static library are included under `RDI/third_party/croaring/`.
All commands below are run from the repository root unless stated otherwise.

```sh
make -C RDI -j2
```

This produces `RDI/rdi`. The default Makefile uses `-march=native`; build on
the machine where the executable will run. See [RDI/README.md](RDI/README.md)
for the input format and execution command.

## Start the Single-Machine Implementation

The vertex file contains one integer ID per line. Each edge line is
`source predicate_id destination`. Convert the released named-predicate
workloads to the numeric IDs used by the encoded graph:

```sh
python3 query/verify.py --emit-numeric /tmp/rpqc-queries
```

For example, run the SF1 core queries with GCNP:

```sh
GCNP_MODE=gcnp QUERY_VARIANT_POLICY=forced RUN_VARIANT_NAME=gcnp \
BOUNDED_SUFFIX_MODE=dual_state BLPC_HYBRID_EVAL_MODE=macro \
./RDI/rdi /path/to/sf1_entity.txt /path/to/sf1_triple.txt \
  /tmp/rpqc-queries/core18/sf1.txt
```

This runs one GCNP variant per query, with dual-state bounded evaluation and
macro evaluation for composite expressions. To run Plain, set `GCNP_MODE=plain`
and `RUN_VARIANT_NAME=plain` in the same command. Other dataset inputs are
`core18/dbpedia.txt`, `core18/yago253.txt`, and `core18/sf3.txt`; robustness
inputs are under `robustness/` in the generated directory.

Results are written under `query_logs/` and `batch_summary.txt` in the current
working directory. Predicate IDs must use the same encoding as the bundled
dataset-specific map.

## Build the Distributed Implementation

Requires GCC, a C++17-capable G++ compiler, POSIX threads, and zlib development
headers. CRoaring source is included under `distribute/vendor/roaring/`.

```sh
bash distribute/build.sh
```

This produces `build/bin/worker`, `build/bin/coordinator`, and
`build/bin/build_partitions_from_v2p` inside `distribute/`. See
[distribute/README.md](distribute/README.md) for partitioning and execution.

## Start the Distributed Implementation

First create the partition directory from an existing METIS vertex-to-partition
mapping. The mapping file contains `vertex partition` pairs; the following
example uses two partitions:

```sh
./distribute/build/bin/build_partitions_from_v2p \
  -v /path/to/vertices.txt -e /path/to/edges.txt \
  -m /path/to/v2p.txt -o /path/to/partitions -n 2
```

Start worker 0 on its assigned host, in a separate terminal:

```sh
./distribute/build/bin/worker \
  -p 0 -n 2 --port 50051 -d /path/to/partitions/partition_0
```

Start worker 1 on its assigned host:

```sh
./distribute/build/bin/worker \
  -p 1 -n 2 --port 50052 -d /path/to/partitions/partition_1
```

After both workers are ready, run the coordinator. Replace `WORKER0_IP` and
`WORKER1_IP` with their reachable addresses; use `127.0.0.1` for both when
running the two workers locally:

```sh
./distribute/build/bin/coordinator \
  -w 'WORKER0_IP:50051,WORKER1_IP:50052' \
  -d /path/to/partitions -q '(39){16}' --query-id 1
```

The expression uses the encoded graph's numeric predicate IDs. Use the verified
numeric query files generated above when selecting expressions from the paper
workloads. For subsequent queries, retain the workers and increase
`--query-id` (`2`, `3`, and so on). The coordinator reports the endpoint-pair
count and execution diagnostics. Workers need their local partition files, and
the coordinator needs the partition metadata and `v2p.txt`.

## Queries and Datasets

[query/README.md](query/README.md) describes the four datasets' 18 core and 90
robustness queries each. Predicate IRIs are enclosed in `<...>`. The current
executables use numeric predicate IDs from the graph encoding; the mapping
and a standalone query verifier are included in `query/`.

```sh
python3 query/verify.py
```

Dataset download sources are listed in [dataset/README.md](dataset/README.md).
