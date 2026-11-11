# Datasets

## Sources

DBpedia 2016-04  
https://downloads.dbpedia.org/2016-04/
Use the mapping-based objects N-Triples export.

LDBC SNB SF1 / SF3  
https://ldbcouncil.org/benchmarks/snb/datasets/

YAGO2s  
https://yago-knowledge.org/downloads/yago-2s

## Preprocessing

The raw datasets are converted into directed, edge-labeled graphs before
execution.

For DBpedia, resource-to-resource RDF triples are retained. Subjects and objects
are mapped to integer vertex IDs, and predicate IRIs are mapped to integer label
IDs. Triples with literal objects are excluded.

For YAGO2s, the YAGO 2.5.3 simple-Turtle export is parsed, resolving prefixes and
relative IRIs. Resource-to-resource triples are retained and encoded in the same
way as DBpedia; literal objects are excluded.

For LDBC SNB SF1 and SF3, vertices are read from the entity CSV tables and edges
from the relationship CSV tables. A vertex is identified by its entity type and
original ID, such as `person:933`, before being assigned an integer ID. Each
relationship type, such as `person_knows_person`, receives a predicate ID.
Literal-valued attributes, email addresses, and language values are excluded.
Relationship direction follows the source and destination columns of the CSV.

The converted files use the following formats. Mapping files are tab-separated
and include a header; vertex and edge files have no header.

| File | Contents |
|---|---|
| `<dataset>_entity.txt` | One integer vertex ID per line |
| `<dataset>_triple.txt` | One `source_id predicate_id destination_id` edge per line |
| `<dataset>_entity_map.tsv` | `entity_id` and original entity identifier |
| `<dataset>_predicate_map.tsv` | `predicate_id` and original predicate IRI or relationship name |

Vertex and predicate IDs are assigned separately for each dataset. Query
predicates must use the same IDs as the converted graph. The released workload
mapping in [`query/predicate-map.tsv`](../query/predicate-map.tsv) records the
predicate IDs used by the included queries. For LDBC, its predicate IRIs use the
`http://sf1/` or `http://sf3/` namespace followed by the relationship name.

## Conversion Commands

Requires Python 3.10 or later, with no third-party packages. Run from the
repository root after extracting the downloads.

DBpedia:

```sh
python3 dataset/convert_dbpedia.py \
  --input-file /path/to/mappingbased_objects_en.nt \
  --output-dir /path/to/converted/dbpedia
```

YAGO2s:

```sh
python3 dataset/convert_yago.py \
  --input-file /path/to/yago-2.5.3-turtle-simple.ttl \
  --output-dir /path/to/converted/yago253
```

LDBC SNB (CsvBasic layout with `dynamic/` and `static/` directories,
pipe-separated columns, and headers):

```sh
python3 dataset/convert_ldbc.py \
  --input-dir /path/to/social_network-sf1-CsvBasic-LongDateFormatter \
  --output-dir /path/to/converted/sf1 --dataset-prefix sf1
```

For SF3, use its input directory, output directory, and `--dataset-prefix sf3`.
The LDBC converter reads all matching CSV shards for each entity and relationship
table. Each converter also writes `<dataset>_stats.json` with conversion counts.

RDF IDs are assigned in first-occurrence order. A different input version or
ordering can change these IDs; use the generated predicate map when encoding
queries rather than assuming that its IDs match the released workload map.
