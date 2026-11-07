#!/usr/bin/env python3
"""Check complete endpoint sets against an independent finite-graph oracle."""
import argparse
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import tempfile


def compose(left, right):
    index = {}
    for mid, dst in right:
        index.setdefault(mid, set()).add(dst)
    return {(src, dst) for src, mid in left for dst in index.get(mid, ())}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('binary', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    edges = [(0, 0, 1), (0, 0, 2), (1, 0, 2), (2, 0, 0),
             (2, 0, 3), (3, 0, 3), (4, 0, 5), (0, 1, 4),
             (1, 1, 3), (3, 1, 6), (5, 1, 1), (6, 1, 6)]
    identity = {(v, v) for v in range(8)}
    a = {(s, t) for s, label, t in edges if label == 0}
    b = {(s, t) for s, label, t in edges if label == 1}

    def power(body, k):
        result = identity
        for _ in range(k):
            result = compose(result, body)
        return result

    def counted(body, lower, upper):
        if upper is not None:
            return set().union(*(power(body, k) for k in range(lower, upper + 1)))
        closure = set(identity)
        while True:
            extended = closure | compose(closure, body)
            if extended == closure:
                break
            closure = extended
        return compose(power(body, lower), closure)

    cases = []
    for text, body in [('0', a), ('0/1', compose(a, b)), ('0|1', a | b)]:
        for lower, upper in [(2, 2), (4, 4), (48, 48), (2, 4), (2, None)]:
            bound = str(lower) if upper == lower else f'{lower},{"" if upper is None else upper}'
            cases.append((f'({text}){{{bound}}}', counted(body, lower, upper)))
    for k in (2, 4):
        left, right = power(a, k), power(compose(a, b), k)
        cases.extend([
            (f'((0){{{k}}}|(0/1){{{k}}}){{2}}', power(left | right, 2)),
            (f'(0){{{k}}}/(1){{{k}}}', compose(left, power(b, k))),
            (f'((0){{{k}}}|(1){{{k}}}){{2,4}}', counted(left | power(b, k), 2, 4)),
            (f'((0){{{k}}}|(1){{{k}}}){{2,}}', counted(left | power(b, k), 2, None)),
        ])
    cases.extend([('1/(0){2,}', compose(b, counted(a, 2, None))),
                  ('(0){2,}/1', compose(counted(a, 2, None), b)),
                  ('(0){2}/(1){2}/0', compose(compose(power(a, 2), power(b, 2)), a))])
    with tempfile.TemporaryDirectory(prefix='rpq-bitmap-oracle-', dir='/tmp') as tmp:
        root = Path(tmp)
        (root/'vertices.txt').write_text(''.join(f'{v}\n' for v in range(8)))
        (root/'edges.txt').write_text(''.join(f'{s} {p} {t}\n' for s, p, t in edges))
        (root/'queries.txt').write_text(''.join(expr+'\n' for expr, _ in cases))
        for export in ('1', '0'):
            logs = output / ('pairs' if export == '1' else 'count-only')
            env = dict(os.environ, GCNP_MODE='gcnp', GCNP_THREADS='1',
                       GCNP_COVERAGE_THREADS='1', QUERY_VARIANT_POLICY='forced',
                       RUN_VARIANT_NAME='gcnp', BOUNDED_SUFFIX_MODE='dual_state',
                       BLPC_HYBRID_EVAL_MODE='macro', RDI_TRACE_UNIONS='1',
                       RDI_EXPORT_PAIRS=export, QUERY_OUTPUT_ROOT=str(logs),
                       QUERY_START_INDEX='1', QUERY_END_INDEX=str(len(cases)),
                       QUERY_TIMEOUT_SECONDS='30')
            with (output/f'runner-export{export}.log').open('w') as stream:
                subprocess.run([str(args.binary.resolve()), str(root/'vertices.txt'),
                                str(root/'edges.txt'), str(root/'queries.txt')],
                               cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT,
                               check=True, timeout=120)
            for q, (expression, expected) in enumerate(cases, 1):
                leaf = logs / f'q{q}' / 'gcnp'
                match = re.search(r'result num:(\d+)', (leaf/'run.log').read_text())
                assert match and int(match[1]) == len(expected), (q, expression, match, len(expected))
                if export == '1':
                    actual = set(struct.iter_unpack('<II', (leaf/'pairs.u32le').read_bytes()))
                    assert actual == expected, (q, expression, actual ^ expected)
                else:
                    assert not (leaf/'pairs.u32le').exists()
    result = dict(status='PASS', queries=len(cases), modes=['complete-pair-set', 'count-only'],
                  binary=str(args.binary.resolve()), expressions=[expr for expr, _ in cases])
    (output/'summary.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
