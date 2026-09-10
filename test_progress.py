"""Exercise the actual progress-logging function with nullable serverless metrics."""
import ast
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
import unittest

class ProgressTests(unittest.TestCase):
    def setUp(self):
        source=(Path(__file__).resolve().parents[1]/'notebooks/00_advanced_common.py').read_text()
        tree=ast.parse(source)
        function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='record_progress')
        self.spark=MagicMock()
        self.delta=MagicMock()
        scope={'json':json,'spark':self.spark,'DeltaTable':self.delta,'name':lambda a,b:f'{a}.{b}'}
        exec(compile(ast.Module(body=[function],type_ignores=[]),'record_progress','exec'),scope)
        self.record=scope['record_progress']

    def test_explicit_none_metrics_and_missing_batch_are_safe(self):
        query=SimpleNamespace(recentProgress=[
            {'batchId':0,'numInputRows':None,'eventTime':None,'stateOperators':[{'numRowsDroppedByWatermark':None}]},
            {'batchId':None,'numInputRows':3},
            {'batchId':1,'numInputRows':0,'stateOperators':None},
        ])
        self.record(query,1,'web_dedup')
        rows=self.spark.createDataFrame.call_args.args[0]
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0][:6],(1,'web_dedup',0,None,'',None))
        self.assertEqual(rows[1][:6],(1,'web_dedup',1,0,'',None))
        self.assertIsNone(json.loads(rows[0][6])['numInputRows'])

    def test_known_zero_and_numeric_values_preserved(self):
        query=SimpleNamespace(recentProgress=[json.dumps({'batchId':2,'numInputRows':4,
            'eventTime':{'watermark':'2026-09-01T10:15:00Z'},
            'stateOperators':[{'numRowsDroppedByWatermark':0},{'numRowsDroppedByWatermark':2}]})])
        self.record(query,4,'web_dedup')
        row=self.spark.createDataFrame.call_args.args[0][0]
        self.assertEqual(row[:6],(4,'web_dedup',2,4,'2026-09-01T10:15:00Z',2))

    def test_uuid_progress_serializes(self):
        identifier=uuid.uuid4()
        self.record(SimpleNamespace(recentProgress=[{'batchId':0,'id':identifier,'numInputRows':None}]),1,'web_dedup')
        row=self.spark.createDataFrame.call_args.args[0][0]
        self.assertEqual(json.loads(row[6])['id'],str(identifier))
        self.assertIsNone(row[3])

    def test_no_progress_does_not_write(self):
        for progress in [None,[],[None], [{'batchId':None}]]:
            self.record(SimpleNamespace(recentProgress=progress),1,'web_dedup')
        self.spark.createDataFrame.assert_not_called()

if __name__=='__main__':unittest.main()
