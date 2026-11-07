# Single-Machine GCNP

RDI evaluates regular path queries with exact, bounded, lower-unbounded, and
nested counting operators. Results contain distinct endpoint pairs over walks.

## Build

Requires GNU Make and a C++17-capable G++ compiler. From the repository root:

```sh
cd RDI
make -j2
```

The Makefile uses the CRoaring headers and static library included under
`third_party/croaring/` and produces `./rdi`. Its default `-march=native` flag
targets the build machine, so rebuild when using another CPU architecture.

## Inputs and Execution

The vertex file contains one integer vertex ID per line. Each edge is a
`source predicate_id destination` triple. The query file contains one
expression per line, using the same numeric predicate IDs as the edge file.
Blank lines and lines starting with `#` are ignored.

```sh
./rdi /path/to/vertices.txt /path/to/edges.txt /path/to/numeric-queries.txt
```

Named-predicate versions of the final workloads and the ID-to-IRI mapping are
in `../query/`. From the repository root, generate numeric execution files with
`python3 query/verify.py --emit-numeric /tmp/rpqc-queries`.
Exact repetition is written `{k}`, bounded repetition `{l,u}`,
and lower-unbounded repetition `{l,}`. Parentheses retain the scope of each
counting operator, including nested expressions.

The default output includes `batch_summary.txt` and `query_logs/` in the
working directory. Use a separate output directory for each run.

## Correctness Check

From `RDI/`, after building:

```sh
python3 tests/check_bitmap_relations.py ./rdi /tmp/rpqc-single-check
```

This checks complete endpoint sets and count-only execution against a finite
graph oracle, including cycles, bounded and unbounded counts, and nesting.
