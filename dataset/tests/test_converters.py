"""Run converters on small raw-data fixtures and check their complete outputs."""
import csv
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


DATASET = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('convert_ldbc', DATASET / 'convert_ldbc.py')
ldbc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ldbc)


class Converters(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='rpqc-converters-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def run_converter(self, script, *args):
        subprocess.run([sys.executable, str(DATASET / script), *map(str, args)],
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def test_dbpedia_resources_literals_and_direction(self):
        source = self.root / 'dbpedia.nt'
        source.write_text('# fixture\n'
                          '<urn:a> <urn:p> <urn:b> .\n'
                          '<urn:b> <urn:q> <urn:b> .\n'
                          '<urn:a> <urn:name> "a literal"@en .\n'
                          '_:node <urn:p> <urn:a> .\n'
                          '<urn:a> <urn:p> <urn:b> .\n', encoding='utf-8')
        output = self.root / 'dbpedia'
        self.run_converter('convert_dbpedia.py', '--input-file', source, '--output-dir', output)
        self.assertEqual((output / 'dbpedia_entity.txt').read_text(), '0\n1\n2\n')
        self.assertEqual((output / 'dbpedia_triple.txt').read_text(), '0 0 1\n1 1 1\n2 0 0\n0 0 1\n')
        self.assertEqual((output / 'dbpedia_entity_map.tsv').read_text(),
                         'entity_id\tentity\n0\turn:a\n1\turn:b\n2\t_:node\n')
        self.assertEqual((output / 'dbpedia_predicate_map.tsv').read_text(),
                         'predicate_id\tpredicate\n0\turn:p\n1\turn:q\n')
        stats = json.loads((output / 'dbpedia_stats.json').read_text())
        self.assertEqual((stats['kept_triples'], stats['skipped_literals']), (4, 1))

    def test_yago_prefixes_base_literals_and_self_loop(self):
        source = self.root / 'yago.ttl'
        source.write_text('@base <http://example.org/> .\n'
                          '@prefix ex: <http://example.org/> .\n'
                          '<a>\tex:p\t<b>\t.\n'
                          'ex:b\trdfs:subClassOf\tex:b\t.\n'
                          'ex:a\tex:name\t"literal"\t.\n', encoding='utf-8')
        output = self.root / 'yago'
        self.run_converter('convert_yago.py', '--input-file', source, '--output-dir', output)
        self.assertEqual((output / 'yago253_triple.txt').read_text(), '0 0 1\n1 1 1\n')
        self.assertEqual((output / 'yago253_entity_map.tsv').read_text(),
                         'entity_id\tentity\n0\thttp://example.org/a\n1\thttp://example.org/b\n')
        self.assertEqual((output / 'yago253_predicate_map.tsv').read_text(),
                         'predicate_id\tpredicate\n0\thttp://example.org/p\n'
                         '1\thttp://www.w3.org/2000/01/rdf-schema#subClassOf\n')
        stats = json.loads((output / 'yago253_stats.json').read_text())
        self.assertEqual((stats['kept_triples'], stats['skipped_literals']), (2, 1))

    def test_ldbc_shards_typed_ids_and_workload_predicates(self):
        source = self.root / 'raw'
        for kind, relative in ldbc.NODE_FILES:
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('id|name\n7|example\n')
        person_shard = source / 'dynamic/person_0_1.csv'
        person_shard.write_text('id|name\n8|second\n')
        expected_edges = []
        ids = {kind: index for index, (kind, _) in enumerate(ldbc.NODE_FILES)}
        ids = {kind: index + (index > 0) for kind, index in ids.items()}
        for predicate, (name, relative) in enumerate(ldbc.EDGE_FILES):
            left, right = name.split('_')[0], name.split('_')[-1]
            path = source / relative
            path.write_text(f'{left}.id|{right}.id\n7|7\n')
            expected_edges.append(f'{ids[left]} {predicate} {ids[right]}\n')
            if name == 'person_knows_person':
                path.with_name('person_knows_person_0_1.csv').write_text('Person.id|Person.id\n8|7\n')
                expected_edges.append(f'1 {predicate} 0\n')
        for _, relative in ldbc.LITERAL_EDGE_FILES:
            (source / relative).write_text('Person.id|value\n7|literal\n')
        with (DATASET.parent / 'query/predicate-map.tsv').open() as stream:
            reference = list(csv.DictReader(stream, delimiter='\t'))
        for dataset in ('sf1', 'sf3'):
            output = self.root / dataset
            self.run_converter('convert_ldbc.py', '--input-dir', source, '--output-dir', output,
                               '--dataset-prefix', dataset)
            self.assertEqual((output / f'{dataset}_entity.txt').read_text(), ''.join(f'{i}\n' for i in range(9)))
            self.assertEqual((output / f'{dataset}_triple.txt').read_text(), ''.join(expected_edges))
            with (output / f'{dataset}_predicate_map.tsv').open() as stream:
                predicates = {row['predicate']: row['predicate_id'] for row in csv.DictReader(stream, delimiter='\t')}
            for row in reference:
                if row['dataset'] == dataset:
                    self.assertEqual(predicates[row['predicate']], row['predicate_id'])
            stats = json.loads((output / f'{dataset}_stats.json').read_text())
            self.assertEqual(stats['entity_count'], 9)
            self.assertEqual(stats['triple_count'], 24)
            self.assertEqual(sum(stats['skipped_literal_edge_counts'].values()), 2)


if __name__ == '__main__':
    unittest.main()
