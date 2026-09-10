"""Offline contract checks; no Spark or Azure runtime is simulated as verified."""
import ast
from collections import defaultdict
from datetime import datetime,timedelta
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
PHASES=json.loads((ROOT/'fixtures.json').read_text())

def histories(events):
    """Independent fixture oracle: ordered transitions with end-exclusive intervals."""
    grouped=defaultdict(list)
    for event in events:
        grouped[event['customer_id']].append(event)
    result={}
    for key,items in grouped.items():
        changes=[]
        for event in sorted(items,key=lambda r:r['sequence']):
            state=(event['region'],event['op']=='D')
            if not changes or changes[-1]['state']!=state:
                changes.append({'sequence':event['sequence'],'state':state,'start':event['effective_at'],'end':None})
        for a,b in zip(changes,changes[1:]): a['end']=b['start']
        result[key]=changes
    return result

class ContractTests(unittest.TestCase):
    def test_all_python_parses(self):
        for p in ROOT.rglob('*.py'):
            ast.parse(p.read_text(),filename=str(p))

    def test_embedded_fixtures_match(self):
        tree=ast.parse((ROOT/'notebooks/10_advanced_demo.py').read_text())
        fixtures=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign)
            and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='PHASES')
        self.assertEqual(fixtures,PHASES)

    def test_scd_history_and_latest_contact_contract(self):
        ledger={}
        rejected=[]
        for phase in PHASES:
            for e in phase['customers']:
                if e['sequence']<=0:
                    rejected.append(e); continue
                if e['event_id'] in ledger: self.assertEqual(ledger[e['event_id']],e)
                ledger[e['event_id']]=e
        h=histories(ledger.values())
        self.assertEqual([x['state'] for x in h['C1']],[('North',False),('West',False),('South',False)])
        self.assertEqual([x['sequence'] for x in h['C1']],[1,2,3])
        self.assertEqual(h['C1'][0]['end'],'2026-09-02T00:00:00Z')
        self.assertEqual(h['C1'][1]['end'],'2026-09-03T00:00:00Z')
        self.assertIsNone(h['C1'][-1]['end'])
        latest=max((e for e in ledger.values() if e['customer_id']=='C1'),key=lambda e:e['sequence'])
        self.assertEqual(latest['email'],'asha.final@example.com')
        self.assertEqual(h['C2'][-1]['state'],(None,True))
        self.assertEqual(len(ledger),6)
        self.assertEqual(len(rejected),1)
        self.assertEqual(sum(len(p['customers']) for p in PHASES),8)

    def test_late_same_region_can_remove_redundant_transition(self):
        # A late B at sequence 2 means the previously known B at 3 is no longer a transition.
        items=[dict(e) for p in PHASES for e in p['customers'] if e['customer_id']=='C1' and e['sequence'] in [1,2,3]]
        dedup={e['event_id']:e for e in items}
        for e in dedup.values():
            if e['sequence']==2: e['region']='South'
        h=histories(dedup.values())['C1']
        self.assertEqual([e['sequence'] for e in h],[1,2])

    def test_watermark_fixture_arrival_contract(self):
        # Contract oracle uses watermark established by earlier phases, not wall-clock time.
        max_time=None
        accepted={}
        late=[]
        for phase in PHASES:
            watermark=max_time-timedelta(minutes=10) if max_time else None
            for event in phase['web']:
                timestamp=datetime.fromisoformat(event['event_time'].replace('Z','+00:00'))
                if watermark is not None and timestamp<=watermark:
                    late.append(event['event_id'])
                elif event['event_id'] not in accepted:
                    accepted[event['event_id']]=(timestamp,event['page'])
            max_time=max(
                ([max_time] if max_time else []) + [datetime.fromisoformat(e['event_time'].replace('Z','+00:00')) for e in phase['web']])
        self.assertEqual(set(accepted),{'e1','e2','e3','e4','e5','e6','e7'})
        self.assertEqual(late,['too-late'])
        final_watermark=max_time-timedelta(minutes=10)
        windows=defaultdict(int)
        for timestamp,page in accepted.values():
            start=timestamp.replace(minute=(timestamp.minute//5)*5,second=0)
            if start+timedelta(minutes=5)<=final_watermark:
                windows[(start.strftime('%H:%M'),page)]+=1
        self.assertEqual(dict(windows),{('10:00','/shop'):3,('10:05','/shop'):1,('10:25','/checkout'):2})
        self.assertEqual(sum(len(p['web']) for p in PHASES),10)

class DeploymentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('deploy',ROOT/'deploy.py')
        cls.module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)

    def test_default_deploy_never_runs_compute(self):
        calls=[]
        def fake(method,endpoint,payload=None,allow_missing=False):
            calls.append((method,endpoint,payload))
            if 'get-status' in endpoint: return None
            if 'jobs/list' in endpoint: return {'jobs':[]}
            if 'jobs/create' in endpoint: return {'job_id':123}
            return {}
        with patch.object(self.module,'api',side_effect=fake):
            self.assertEqual(self.module.deploy(),123)
        self.assertFalse(any('run-now' in e for _,e,_ in calls))
        imports=[p for _,e,p in calls if e.endswith('/import')]
        self.assertEqual(len(imports),2)
        self.assertTrue(all(p['overwrite'] is False for p in imports))
        job=next(p for _,e,p in calls if e.endswith('jobs/create'))
        self.assertNotIn('schedule',job)
        self.assertEqual(job['timeout_seconds'],1800)
        self.assertEqual(job['performance_target'],'STANDARD')

    def test_existing_modified_notebook_is_not_overwritten(self):
        import base64
        calls=[]
        def fake(method,endpoint,payload=None,allow_missing=False):
            calls.append((method,endpoint))
            if 'get-status' in endpoint: return {'object_type':'NOTEBOOK'}
            if '/export?' in endpoint: return {'content':base64.b64encode(b'changed notebook').decode()}
            return {}
        with patch.object(self.module,'api',side_effect=fake):
            with self.assertRaisesRegex(RuntimeError,'different content'):
                self.module.deploy()
        self.assertFalse(any(e.endswith('/import') for _,e in calls))

if __name__=='__main__':unittest.main()
