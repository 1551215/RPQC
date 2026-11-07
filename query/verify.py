#!/usr/bin/env python3
"""Verify released named-predicate queries against frozen numeric workloads."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
TOKEN = re.compile(r'<[^<>\s]+>|[0-9]+|[()/|{},]')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class Parser:
    def __init__(self, text):
        self.tokens = []
        self.position = 0
        cursor = 0
        for match in TOKEN.finditer(text):
            if match.start() != cursor:
                raise ValueError(f'Unexpected character at offset {cursor}: {text!r}')
            self.tokens.append(match.group())
            cursor = match.end()
        if cursor != len(text):
            raise ValueError(f'Unparsed suffix at offset {cursor}: {text!r}')

    def take(self, token):
        if self.position < len(self.tokens) and self.tokens[self.position] == token:
            self.position += 1
            return True
        return False

    def expect(self, token):
        if not self.take(token):
            raise ValueError(f'Expected {token!r} at token {self.position}')

    def number(self):
        if self.position >= len(self.tokens) or not self.tokens[self.position].isdigit():
            raise ValueError(f'Expected count at token {self.position}')
        value = int(self.tokens[self.position])
        self.position += 1
        return value

    def alternative(self):
        self.sequence()
        while self.take('|'):
            self.sequence()

    def sequence(self):
        self.factor()
        while self.take('/'):
            self.factor()

    def factor(self):
        if self.take('('):
            self.alternative()
            self.expect(')')
        elif self.position < len(self.tokens) and self.tokens[self.position].startswith('<'):
            self.position += 1
        else:
            raise ValueError(f'Expected named predicate or group at token {self.position}')
        if self.take('{'):
            lower = self.number()
            upper = lower
            if self.take(','):
                upper = None if self.position < len(self.tokens) and self.tokens[self.position] == '}' else self.number()
            if lower < 1 or upper is not None and upper < lower:
                raise ValueError('Invalid repetition bounds')
            self.expect('}')

    def parse(self):
        self.alternative()
        if self.position != len(self.tokens):
            raise ValueError(f'Unexpected token at position {self.position}')


def verify(emit_numeric=None):
    manifest = json.loads((HERE/'manifest.json').read_text())
    map_path = HERE/manifest['predicate_map']['file']
    assert digest(map_path.read_bytes()) == manifest['predicate_map']['sha256'], 'Predicate map checksum mismatch'
    maps = {}
    with map_path.open(newline='') as stream:
        for row in csv.DictReader(stream, delimiter='\t'):
            mapping = maps.setdefault(row['dataset'], {})
            token = '<'+row['iri']+'>'
            assert token not in mapping, ('Ambiguous predicate', row)
            assert row['predicate_id'].isdigit(), row
            mapping[token] = row['predicate_id']
    total = 0
    numeric_files = {}
    for item in manifest['files']:
        path = HERE/item['file']
        raw = path.read_bytes()
        assert digest(raw) == item['sha256'], f'Named query checksum mismatch: {path}'
        text = raw.decode('ascii')
        assert text.endswith('\n') and '\r' not in text, path
        expressions = text.splitlines()
        assert len(expressions) == item['query_count'], path
        numeric = []
        for q, expression in enumerate(expressions, 1):
            try:
                parser = Parser(expression)
                parser.parse()
                numeric.append(''.join(maps[item['dataset']][token] if token.startswith('<') else token
                                       for token in parser.tokens))
            except (ValueError, KeyError) as error:
                raise ValueError(f'{path.name} Q{q}: {error}') from error
        restored = ('\n'.join(numeric)+'\n').encode('ascii')
        assert digest(restored) == item['numeric_source_sha256'], f'Frozen numeric input mismatch: {path}'
        numeric_files[item['file']] = restored
        total += len(expressions)
        print(f"PASS {item['file']}: {len(expressions)} queries, exact numeric round trip")
    assert len(manifest['files']) == 8 and total == 432, total
    print('PASS: all 432 expressions preserve the frozen predicates, operators, bounds, and query order')
    if emit_numeric is not None:
        destination = emit_numeric.resolve()
        for relative, raw in numeric_files.items():
            path = destination/relative
            if path.exists() and path.read_bytes() != raw:
                raise FileExistsError(f'Refusing to overwrite different query content: {path}')
        for relative, raw in numeric_files.items():
            path = destination/relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        print(f'Numeric execution inputs written to {destination}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--emit-numeric', type=Path,
                        help='Also write verified numeric-ID execution inputs to this directory')
    verify(parser.parse_args().emit_numeric)
