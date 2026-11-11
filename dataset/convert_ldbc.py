#!/usr/bin/env python3
"""Convert LDBC SNB CsvBasic files into integer graph files.

Outputs in --output-dir:
  - <prefix>_entity.txt
  - <prefix>_entity_map.tsv
  - <prefix>_predicate_map.tsv
  - <prefix>_triple.txt

Literal-valued relations are excluded by default, so only entity-to-entity
edges are kept.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re


NODE_FILES: tuple[tuple[str, str], ...] = (
    ("person", "dynamic/person_0_0.csv"),
    ("forum", "dynamic/forum_0_0.csv"),
    ("post", "dynamic/post_0_0.csv"),
    ("comment", "dynamic/comment_0_0.csv"),
    ("tag", "static/tag_0_0.csv"),
    ("tagclass", "static/tagclass_0_0.csv"),
    ("place", "static/place_0_0.csv"),
    ("organisation", "static/organisation_0_0.csv"),
)

EDGE_FILES: tuple[tuple[str, str], ...] = (
    ("person_isLocatedIn_place", "dynamic/person_isLocatedIn_place_0_0.csv"),
    ("person_hasInterest_tag", "dynamic/person_hasInterest_tag_0_0.csv"),
    ("person_workAt_organisation", "dynamic/person_workAt_organisation_0_0.csv"),
    ("person_studyAt_organisation", "dynamic/person_studyAt_organisation_0_0.csv"),
    ("person_knows_person", "dynamic/person_knows_person_0_0.csv"),
    ("forum_containerOf_post", "dynamic/forum_containerOf_post_0_0.csv"),
    ("forum_hasMember_person", "dynamic/forum_hasMember_person_0_0.csv"),
    ("forum_hasModerator_person", "dynamic/forum_hasModerator_person_0_0.csv"),
    ("forum_hasTag_tag", "dynamic/forum_hasTag_tag_0_0.csv"),
    ("post_hasCreator_person", "dynamic/post_hasCreator_person_0_0.csv"),
    ("post_hasTag_tag", "dynamic/post_hasTag_tag_0_0.csv"),
    ("post_isLocatedIn_place", "dynamic/post_isLocatedIn_place_0_0.csv"),
    ("comment_hasCreator_person", "dynamic/comment_hasCreator_person_0_0.csv"),
    ("comment_hasTag_tag", "dynamic/comment_hasTag_tag_0_0.csv"),
    ("comment_isLocatedIn_place", "dynamic/comment_isLocatedIn_place_0_0.csv"),
    ("comment_replyOf_post", "dynamic/comment_replyOf_post_0_0.csv"),
    ("comment_replyOf_comment", "dynamic/comment_replyOf_comment_0_0.csv"),
    ("person_likes_post", "dynamic/person_likes_post_0_0.csv"),
    ("person_likes_comment", "dynamic/person_likes_comment_0_0.csv"),
    ("tag_hasType_tagclass", "static/tag_hasType_tagclass_0_0.csv"),
    ("tagclass_isSubclassOf_tagclass", "static/tagclass_isSubclassOf_tagclass_0_0.csv"),
    ("place_isPartOf_place", "static/place_isPartOf_place_0_0.csv"),
    ("organisation_isLocatedIn_place", "static/organisation_isLocatedIn_place_0_0.csv"),
)

LITERAL_EDGE_FILES: tuple[tuple[str, str], ...] = (
    ("person_email_emailaddress", "dynamic/person_email_emailaddress_0_0.csv"),
    ("person_speaks_language", "dynamic/person_speaks_language_0_0.csv"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        required=True,
    )
    parser.add_argument(
        "--output-dir",
        required=True,
    )
    parser.add_argument(
        "--dataset-prefix",
        default="sf1",
    )
    return parser.parse_args()


def count_data_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as fh:
        next(fh)
        return sum(1 for line in fh if line.rstrip("\n"))


def csv_paths(input_dir: Path, rel_path: str, required: bool = True) -> list[Path]:
    table = Path(rel_path)
    stem = table.name.removesuffix('_0_0.csv')
    pattern = str(table.parent / (stem + '_*.csv'))
    filename = re.compile(re.escape(stem) + r'_\d+_\d+\.csv')
    paths = sorted(path for path in input_dir.glob(pattern) if filename.fullmatch(path.name))
    if required and not paths:
        raise FileNotFoundError(f'No CSV files matching {input_dir / pattern}')
    return paths


def csv_rows(input_dir: Path, rel_path: str):
    for path in csv_paths(input_dir, rel_path):
        with path.open('r', encoding='utf-8', newline='') as stream:
            reader = csv.reader(stream, delimiter='|')
            header = next(reader)
            for row in reader:
                if row:
                    yield header, row


def write_entity_file(path: Path, entity_count: int) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for entity_id in range(entity_count):
            fh.write(f"{entity_id}\n")


def write_mapping(path: Path, rows: list[tuple[int, str]], header: str) -> None:
    with path.open("w", encoding="utf-8") as fh:
        fh.write(header + "\n")
        for row_id, value in rows:
            fh.write(f"{row_id}\t{value}\n")


def load_nodes(input_dir: Path) -> tuple[dict[str, int], list[tuple[int, str]], dict[str, int]]:
    entity_ids: dict[str, int] = {}
    entity_rows: list[tuple[int, str]] = []
    node_counts: dict[str, int] = {}

    for node_type, rel_path in NODE_FILES:
        count = 0
        for header, row in csv_rows(input_dir, rel_path):
            id_idx = header.index("id")
            raw_id = row[id_idx]
            key = f"{node_type}:{raw_id}"
            if key in entity_ids:
                raise ValueError(f'Duplicate entity: {key}')
            entity_id = len(entity_ids)
            entity_ids[key] = entity_id
            entity_rows.append((entity_id, key))
            count += 1
        node_counts[node_type] = count

    return entity_ids, entity_rows, node_counts


def convert_edges(
    input_dir: Path,
    triple_path: Path,
    entity_ids: dict[str, int],
) -> tuple[dict[str, int], dict[str, int]]:
    predicate_ids = {name: idx for idx, (name, _path) in enumerate(EDGE_FILES)}
    edge_counts: dict[str, int] = {}

    with triple_path.open("w", encoding="utf-8") as out:
        for pred_name, rel_path in EDGE_FILES:
            pred_id = predicate_ids[pred_name]
            count = 0
            for header, row in csv_rows(input_dir, rel_path):
                src_col = header[0]
                dst_col = header[1]
                src_type = src_col.split(".", 1)[0].lower()
                dst_type = dst_col.split(".", 1)[0].lower()
                src_key = f"{src_type}:{row[0]}"
                dst_key = f"{dst_type}:{row[1]}"
                src_id = entity_ids[src_key]
                dst_id = entity_ids[dst_key]
                out.write(f"{src_id} {pred_id} {dst_id}\n")
                count += 1
            edge_counts[pred_name] = count

    return predicate_ids, edge_counts


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    entity_path = output_dir / f"{args.dataset_prefix}_entity.txt"
    entity_map_path = output_dir / f"{args.dataset_prefix}_entity_map.tsv"
    predicate_map_path = output_dir / f"{args.dataset_prefix}_predicate_map.tsv"
    triple_path = output_dir / f"{args.dataset_prefix}_triple.txt"
    stats_path = output_dir / f"{args.dataset_prefix}_stats.json"

    entity_ids, entity_rows, node_counts = load_nodes(input_dir)
    predicate_ids, edge_counts = convert_edges(input_dir, triple_path, entity_ids)

    write_entity_file(entity_path, len(entity_ids))
    write_mapping(entity_map_path, entity_rows, "entity_id\tentity")
    write_mapping(
        predicate_map_path,
        sorted(((pid, name) for name, pid in predicate_ids.items()), key=lambda item: item[0]),
        "predicate_id\tpredicate",
    )

    skipped_literal_edges = {
        name: sum(count_data_rows(path) for path in csv_paths(input_dir, rel_path, required=False))
        for name, rel_path in LITERAL_EDGE_FILES
    }

    stats = {
        "input_dir": str(input_dir),
        "output_dir": str(output_dir),
        "entity_file": str(entity_path),
        "entity_map_file": str(entity_map_path),
        "predicate_map_file": str(predicate_map_path),
        "triple_file": str(triple_path),
        "entity_count": len(entity_ids),
        "triple_count": sum(edge_counts.values()),
        "node_counts": node_counts,
        "edge_counts": edge_counts,
        "skipped_literal_edge_counts": skipped_literal_edges,
    }
    stats_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")

    print(f"[done] entity_count={len(entity_ids)} triple_count={sum(edge_counts.values())}")
    print(f"[done] triple_file={triple_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
