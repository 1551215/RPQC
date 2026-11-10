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

Ordinary and nested bounded evaluation use mirror dual-state by default in
both GCNP and Plain. With `P_k = A^k` and `S_k = I union A union ... union A^k`, its
suffix recurrence is `S_(a+b) = S_b union S_a P_b`. When `P_(a+b)` is already
required, it reuses that power and computes `(S_a minus P_a) P_b` for the
remaining contribution. Nested bodies are evaluated recursively as relations;
their bounded suffixes use the same mirror identity and share exact-power row
caches. Suffixes alone do not force construction of unneeded exact powers.
Exact repetition keeps its existing implementation.

No extra environment flags are needed to enable mirror evaluation.
`BOUNDED_SUFFIX_MODE=dual_state` selects it explicitly;
`DUAL_STATE_DELTA_MODE=off` restores the original suffix recurrence for
comparison. `BOUNDED_SUFFIX_MODE=naive_union` retains the separate bounded
ablation. Optional `DUAL_STATE_DELTA_TRACE=1` enables detailed mirror logging.

The default output includes `batch_summary.txt` and `query_logs/` in the
working directory. Use a separate output directory for each run.

## Correctness Check

From `RDI/`, after building:

```sh
python3 tests/check_bitmap_relations.py ./rdi /tmp/rpqc-single-check
```

This checks complete endpoint sets and count-only execution against a finite
graph oracle, including cycles, zero bounds, `{2,16}`, `{2,64}`, unbounded
counts, depth-1 through depth-5 bounded nesting, and nested counts followed by
another counted expression. It verifies that ordinary and nested mirror paths
execute by default.
Use `--compose-mode plain` to check Plain with the same recurrence, or
`--bounded-recurrence original` to check the original recurrence.
