"""Offline checks only: do not claim Spark execution from these tests."""
import ast
import json
from pathlib import Path
from decimal import Decimal
import unittest

ROOT = Path(__file__).resolve().parents[1]

def rows(batch,entity):
    p=ROOT/'data'/batch/f'{entity}.json'
    return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []

class PackageTests(unittest.TestCase):
    def test_notebook_syntax(self):
        for p in (ROOT/'notebooks').glob('*.py'):
            ast.parse(p.read_text(),filename=str(p))

    def test_seed_matches_downloadable_fixtures(self):
        tree=ast.parse((ROOT/'notebooks/02_seed_demo.py').read_text())
        data={n.targets[0].id:ast.literal_eval(n.value) for n in tree.body
              if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)
              and n.targets[0].id in ['INITIAL','INCREMENTAL']}
        for batch in ['initial','incremental']:
            for entity in ['regions','customers','products','orders']:
                self.assertEqual(data[batch.upper()][entity],rows(batch,entity))

    def test_fixture_expected_totals_and_edge_cases(self):
        current={}
        for batch,total in [('initial','250.00'),('incremental','400.00')]:
            for row in rows(batch,'orders'):
                if row['op']=='U' and int(row['quantity'])<=0:
                    continue
                key=row['order_id']
                if key not in current or int(row['sequence'])>int(current[key]['sequence']):
                    current[key]=row
            active=[r for r in current.values() if r['op']=='U']
            self.assertEqual(len(active),2)
            self.assertEqual(sum(Decimal(r['unit_price'])*int(r['quantity']) for r in active),Decimal(total))
        self.assertEqual(current['O2']['op'],'D')
        self.assertEqual(current['O1']['quantity'],'3')
        self.assertNotIn('BAD1',current)
        inc=rows('incremental','orders')
        self.assertLess(len({json.dumps(r,sort_keys=True) for r in inc}),len(inc))

    def test_bundle_dependency_paths(self):
        import yaml
        data=yaml.safe_load((ROOT/'databricks.yml').read_text())
        job=data['resources']['jobs']['retail_pipeline']
        self.assertEqual(job['max_concurrent_runs'],1)
        self.assertNotIn('schedule',job)
        previous=None
        for task in job['tasks']:
            self.assertTrue((ROOT/task['notebook_task']['notebook_path']).exists())
            if previous:
                self.assertEqual(task['depends_on'],[{'task_key':previous}])
            previous=task['task_key']

if __name__=='__main__':
    unittest.main()
