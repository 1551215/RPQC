#!/usr/bin/env python3
"""Convert DBpedia resource-to-resource N-Triples into integer graph files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

from convert_yago import get_or_add_id, write_entity_file, write_mapping


RESOURCE = r'(?:<[^>]*>|_:[^\s]+)'
TRIPLE = re.compile(rf'^({RESOURCE})\s+(<[^>]*>)\s+({RESOURCE})\s*\.\s*(?:#.*)?$')


def parse_resource(token: str) -> str:
    return token[1:-1] if token.startswith('<') else token


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-file', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--dataset-prefix', default='dbpedia')
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    entities: dict[str, int] = {}
    predicates: dict[str, int] = {}
    prefix = args.dataset_prefix
    triple_path = args.output_dir / f'{prefix}_triple.txt'
    total_lines = kept_triples = skipped_literals = skipped_invalid = 0
    with args.input_file.open(encoding='utf-8') as source, triple_path.open('w', encoding='utf-8') as output:
        for raw in source:
            total_lines += 1
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            match = TRIPLE.fullmatch(line)
            if match is None:
                parts = line.split(None, 2)
                if len(parts) == 3 and parts[2].startswith('"'):
                    skipped_literals += 1
                else:
                    skipped_invalid += 1
                continue
            subject, predicate, obj = map(parse_resource, match.groups())
            source_id = get_or_add_id(entities, subject)
            target_id = get_or_add_id(entities, obj)
            predicate_id = get_or_add_id(predicates, predicate)
            output.write(f'{source_id} {predicate_id} {target_id}\n')
            kept_triples += 1

    write_entity_file(args.output_dir / f'{prefix}_entity.txt', len(entities))
    write_mapping(args.output_dir / f'{prefix}_entity_map.tsv', entities, 'entity_id\tentity')
    write_mapping(args.output_dir / f'{prefix}_predicate_map.tsv', predicates, 'predicate_id\tpredicate')
    stats = dict(input_file=str(args.input_file), entity_count=len(entities),
                 predicate_count=len(predicates), total_lines=total_lines,
                 kept_triples=kept_triples, skipped_literals=skipped_literals,
                 skipped_invalid_lines=skipped_invalid)
    (args.output_dir / f'{prefix}_stats.json').write_text(json.dumps(stats, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(stats))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
