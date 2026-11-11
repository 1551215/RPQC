#!/usr/bin/env python3
"""Convert the YAGO 2.5.3 simple-turtle export into OPattern dataset files.

Input format: the tab-separated YAGO 2.5.3 simple-Turtle export,
"subject\\tpredicate\\tobject\\t.\\n" per line, plus leading `#` comments and
`@base`/`@prefix` declarations. Subjects/objects/predicates are either:
  - <full URI> or <relative-to-base URI> (base is
    http://yago-knowledge.org/resource/)
  - prefix:local (rdf:, rdfs:, owl:, xsd:, skos:)
  - a literal ("value" / "value"^^<datatype> / "value"@lang) -- these are
    only ever objects, and rdf:type objects are usually class resources, not
    literals

Only resource-to-resource triples become graph edges; literal objects are dropped.

Outputs in --output-dir:
  - yago253_entity.txt:        one compact entity id per line (0..N-1)
  - yago253_entity_map.tsv:    "entity_id\\tentity" (full URI), header included
  - yago253_predicate_map.tsv: "predicate_id\\tpredicate" (full URI), header
                                 included
  - yago253_triple.txt:        "subject_id predicate_id object_id" per line
  - yago253_stats.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

DEFAULT_PREFIXES = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-file",
        required=True,
    )
    parser.add_argument(
        "--output-dir",
        required=True,
    )
    parser.add_argument(
        "--dataset-prefix",
        default="yago253",
    )
    return parser.parse_args()


def expand_resource(token: str, base: str, prefixes: dict[str, str]) -> str | None:
    token = token.strip()
    if not token:
        return None
    if token.startswith("<") and token.endswith(">"):
        inner = token[1:-1]
        if inner == "":
            return base
        if "://" in inner:
            return inner  # already absolute
        return base + inner  # relative to @base
    if token.startswith("_:"):
        return token
    if ":" in token:
        prefix, local = token.split(":", 1)
        expanded_base = prefixes.get(prefix)
        if expanded_base is not None:
            return expanded_base + local
        return None
    return None


def parse_line(
    line: str, base: str, prefixes: dict[str, str]
) -> tuple[str, str, str] | None:
    line = line.rstrip("\n")
    if not line or line.startswith("#") or line.startswith("@"):
        return None
    if not line.endswith("\t."):
        return None
    body = line[: -len("\t.")]
    parts = body.split("\t")
    if len(parts) != 3:
        return None
    subj_token, pred_token, obj_token = parts

    subj = expand_resource(subj_token, base, prefixes)
    pred = expand_resource(pred_token, base, prefixes)
    if subj is None or pred is None:
        return None

    obj_token = obj_token.strip()
    if obj_token.startswith('"'):
        return None  # literal object, not a graph edge
    obj = expand_resource(obj_token, base, prefixes)
    if obj is None:
        return None
    return subj, pred, obj


def get_or_add_id(mapping: dict[str, int], key: str) -> int:
    existing = mapping.get(key)
    if existing is not None:
        return existing
    new_id = len(mapping)
    mapping[key] = new_id
    return new_id


def write_mapping(path: Path, mapping: dict[str, int], header: str) -> None:
    with path.open("w", encoding="utf-8") as fh:
        fh.write(header + "\n")
        for key, value in sorted(mapping.items(), key=lambda item: item[1]):
            fh.write(f"{value}\t{key}\n")


def write_entity_file(path: Path, entity_count: int) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for entity_id in range(entity_count):
            fh.write(f"{entity_id}\n")


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = args.dataset_prefix

    base = "http://yago-knowledge.org/resource/"
    prefixes = dict(DEFAULT_PREFIXES)

    entity_ids: dict[str, int] = {}
    predicate_ids: dict[str, int] = {}
    triple_path = output_dir / f"{prefix}_triple.txt"

    total_lines = 0
    kept_triples = 0
    skipped_literals = 0
    skipped_invalid = 0

    print(f"[convert] input={input_path}", flush=True)
    print(f"[convert] output={triple_path}", flush=True)

    with input_path.open("r", encoding="utf-8") as fh, triple_path.open(
        "w", encoding="utf-8"
    ) as triple_out:
        for raw_line in fh:
            total_lines += 1
            if raw_line.startswith("@"):
                if raw_line.startswith("@prefix"):
                    parts = raw_line.split()
                    if len(parts) >= 3:
                        pfx = parts[1].rstrip(":")
                        uri = parts[2].strip("<>")
                        prefixes[pfx] = uri
                elif raw_line.startswith("@base"):
                    parts = raw_line.split()
                    if len(parts) >= 2:
                        base = parts[1].strip("<>")
                continue

            parsed = parse_line(raw_line, base, prefixes)
            if parsed is None:
                stripped = raw_line.strip()
                if not stripped or stripped.startswith("#"):
                    skipped_invalid += 1
                elif '"' in stripped:
                    skipped_literals += 1
                else:
                    skipped_invalid += 1
                continue

            subj, pred, obj = parsed
            subj_id = get_or_add_id(entity_ids, subj)
            obj_id = get_or_add_id(entity_ids, obj)
            pred_id = get_or_add_id(predicate_ids, pred)
            triple_out.write(f"{subj_id} {pred_id} {obj_id}\n")
            kept_triples += 1

            if kept_triples % 5_000_000 == 0:
                print(
                    "[convert] "
                    f"kept_triples={kept_triples} entities={len(entity_ids)} "
                    f"predicates={len(predicate_ids)}",
                    flush=True,
                )

    entity_map_path = output_dir / f"{prefix}_entity_map.tsv"
    entity_txt_path = output_dir / f"{prefix}_entity.txt"
    predicate_map_path = output_dir / f"{prefix}_predicate_map.tsv"
    print(f"[convert] writing {entity_map_path} and {entity_txt_path}", flush=True)
    write_mapping(entity_map_path, entity_ids, "entity_id\tentity")
    write_entity_file(entity_txt_path, len(entity_ids))
    print(f"[convert] writing {predicate_map_path}", flush=True)
    write_mapping(predicate_map_path, predicate_ids, "predicate_id\tpredicate")

    stats = {
        "input_file": str(input_path),
        "output_dir": str(output_dir),
        "entity_map_file": str(entity_map_path),
        "predicate_map_file": str(predicate_map_path),
        "triple_file": str(triple_path),
        "total_lines": total_lines,
        "kept_triples": kept_triples,
        "skipped_literals": skipped_literals,
        "skipped_invalid_lines": skipped_invalid,
        "entity_count": len(entity_ids),
        "predicate_count": len(predicate_ids),
    }
    stats_path = output_dir / f"{prefix}_stats.json"
    stats_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")

    print(
        "[done] "
        f"entities={len(entity_ids)} predicates={len(predicate_ids)} "
        f"triples={kept_triples} skipped_literals={skipped_literals} "
        f"skipped_invalid={skipped_invalid}",
        flush=True,
    )
    print(f"[done] triple_file={triple_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
