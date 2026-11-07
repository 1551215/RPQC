# Paper Query Workloads

These are the final query expressions for the four datasets. Predicate tokens
are the full IRIs used in the RDF graph exports, enclosed in `<...>`. Operators
inside an IRI are part of that predicate, not path operators.

| Dataset | Core workload, Q1-Q18 | Robustness workload, Q1-Q90 |
|---|---|---|
| DBpedia | [core18/dbpedia.txt](core18/dbpedia.txt) | [robustness/dbpedia.txt](robustness/dbpedia.txt) |
| YAGO2s | [core18/yago253.txt](core18/yago253.txt) | [robustness/yago253.txt](robustness/yago253.txt) |
| SF1 | [core18/sf1.txt](core18/sf1.txt) | [robustness/sf1.txt](robustness/sf1.txt) |
| SF3 | [core18/sf3.txt](core18/sf3.txt) | [robustness/sf3.txt](robustness/sf3.txt) |

Each text file contains one expression per line, with no headers or query ID
columns. Line N is query QN. The two workloads have independent query numbering.
There are 72 core queries and 360 robustness queries, 432 expressions in total.

## Classes and Semantics

| Class | Core IDs | Robustness IDs | Type |
|---|---|---|---|
| C1 | Q1-Q3 | Q1-Q15 | Single-predicate exact counting |
| C2 | Q4-Q6 | Q16-Q30 | Sequence-body exact counting |
| C3 | Q7-Q9 | Q31-Q45 | Choice-body exact counting |
| C4 | Q10-Q12 | Q46-Q60 | Bounded counting |
| C5 | Q13-Q15 | Q61-Q75 | Lower-unbounded counting |
| C6 | Q16-Q18 | Q76-Q90 | Composite and nested counting |

`/` is concatenation, `|` is choice, `{k}` is exact repetition, `{l,u}` is
bounded repetition, and `{l,}` is lower-unbounded repetition. Repetition applies
to the complete parenthesized body. Queries use walk semantics, permitting
repeated vertices and edges, and return distinct endpoint pairs.

For example, the core SF1 Q1 expression is
`(<http://sf1/person_knows_person>){16}`. Its repetition bound is still numeric;
only the predicate identifier has been replaced. SF1 and SF3 use the respective
`http://sf1/` and `http://sf3/` namespaces of their RDF exports.

## Sources and Verification

The core lists match the frozen inputs of the latest full-graph single-machine
experiment and the accepted `claude-ai` distributed experiment. The robustness
lists match the final 360-query workload used in the completed GCNP robustness
experiment. In particular, final YAGO2s robustness Q65 corresponds to
`(20|21|22){24,}`; the obsolete Q65 expression is not used.

`predicate-map.tsv` includes every predicate used in these lists, with its
dataset, numeric ID, original map entry, and exported IRI. `manifest.json`
records input/output checksums and source paths relative to the original
experiment workspace. The final core workload is in `core18/`.

Run the following from the repository root:

```sh
python3 query/verify.py
```

The verifier uses only the Python standard library. It parses every expression,
checks all named predicates against the bundled map, converts the expressions
back to numeric IDs, and checks the original frozen input checksums. This also
checks query order, all bounds, parentheses, choice/concatenation operators,
and nested repetitions. No dataset or benchmark result directory is needed.

The current RPQC executables read numeric-ID expressions. To generate execution
inputs after verifying the named files, use:

```sh
python3 query/verify.py --emit-numeric /tmp/rpqc-queries
```

The output directory has the same `core18/` and `robustness/` layout, with
numeric predicates restored from the bundled mapping. Existing files with
different contents are not overwritten. The released files are expressions,
not complete SPARQL queries.
