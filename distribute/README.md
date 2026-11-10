# Distributed GCNP

The distributed implementation runs one worker per graph partition and a
coordinator for each query. It retains GCNP, source work balancing, coverage,
and Biclique + Residual message compression.

Bounded counting uses mirror dual-state by default: with `P_k = A^k` and
`S_k = I union A union ... union A^k`, it computes
`S_(a+b) = S_b union S_a P_b`. When the exact power is already required by
the mirror schedule, it reuses `P_(a+b)` and joins `(S_a minus P_a)` with
`P_b`. Suffix states stay on the original source domain; exact-power requests
expand to the needed mids. Every worker participates in each stage, including
empty inputs. Results retain their existing hash-based source homes.

## Build

Requires GCC, a C++17-capable G++ compiler, POSIX threads, and zlib development
headers. From the repository root:

```sh
cd distribute
bash build.sh
```

Outputs are `build/bin/worker`, `build/bin/coordinator`, and
`build/bin/build_partitions_from_v2p`. The default compiler options include
`-O3 -mtune=generic`. Build paths and compilers can be set with `BUILD_DIR`,
`OUTPUT_DIR`, `CC`, and `CXX`.

## Partition Files

A partition root contains `meta.txt`, `v2p.txt`, and directories named
`partition_0` through `partition_p-1`. Generate these files from an existing
vertex-to-partition mapping, such as the output of METIS:

```sh
./build/bin/build_partitions_from_v2p \
  -v /path/to/vertices.txt -e /path/to/edges.txt \
  -m /path/to/v2p.txt -o /path/to/partitions -n 2
```

The vertex file contains one integer ID per line; edges are
`source predicate_id destination`; the mapping is `vertex partition`.
`partition_modulo.sh` is also available for creating simple local fixtures.
The METIS mapping must be supplied when reproducing the partitioned workloads.

## Execute a Query

Start one resident worker on each assigned host:

```sh
./build/bin/worker -p 0 -n 2 --port 50051 -d /path/to/partitions/partition_0
./build/bin/worker -p 1 -n 2 --port 50052 -d /path/to/partitions/partition_1
```

Once the workers are ready, start the coordinator. For two local workers:

```sh
./build/bin/coordinator \
  -w 127.0.0.1:50051,127.0.0.1:50052 \
  -d /path/to/partitions -q '(39){16}' --query-id 1
```

Use the actual worker addresses when running across machines. Predicates in
the `-q` expression are numeric IDs from the graph encoding; named versions
of the final workloads and the corresponding mapping are in `../query/`.

Workers retain the loaded graph across queries. Use a fresh `--query-id`
for each query; initialization resets query-local relations, caches, and
dependencies. The coordinator reports the distinct endpoint-pair count and
execution diagnostics in its output. Worker logs also report communication
and compression counters; process-lifetime counters require per-query deltas.

Computation, coverage, message preparation, compression, and final result
processing each use one thread per worker. The RPC socket timeout defaults
to 300 seconds and can be set with `SIMPLE_RPC_TIMEOUT_SEC`; it is separate
from any query deadline enforced by the calling process.
