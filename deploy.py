#!/usr/bin/env python3
"""Run in Azure Cloud Shell. Deploy by default; --run explicitly starts compute."""
import argparse
import os
import ast
import base64
import json
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlencode

HOST = os.environ.get('DATABRICKS_HOST', '').rstrip('/')
RESOURCE = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'
FOLDER = '/Shared/azure-retail-advanced'
JOB_NAME = 'vishnu-retail-advanced-demo'
ROOT = Path(__file__).resolve().parent

def api(method,endpoint,payload=None,allow_missing=False):
    if not HOST.startswith('https://'):
        raise RuntimeError('Set DATABRICKS_HOST to your HTTPS workspace URL.')
    cmd=['az','rest','--method',method,'--resource',RESOURCE,'--url',HOST+endpoint,'--output','json']
    if payload is not None:
        cmd += ['--headers','Content-Type=application/json','--body',json.dumps(payload)]
    r=subprocess.run(cmd,capture_output=True,text=True)
    if r.returncode:
        if allow_missing and 'RESOURCE_DOES_NOT_EXIST' in r.stderr:
            return None
        raise RuntimeError(r.stderr.strip())
    return json.loads(r.stdout or '{}')

def desired_job():
    return {
        'name':JOB_NAME, 'max_concurrent_runs':1, 'timeout_seconds':1800,
        'performance_target':'STANDARD',
        'tasks':[{'task_key':'advanced_demo','timeout_seconds':1800,'max_retries':0,
            'notebook_task':{'notebook_path':FOLDER+'/10_advanced_demo','source':'WORKSPACE',
                'base_parameters':{'catalog':os.environ.get('RETAIL_CATALOG','workspace'),'prefix':'vishnu_retail','demo_id':'advanced_v1'}}}]
    }

def find_job():
    matches=[]
    cursor=None
    while True:
        query={'name':JOB_NAME,'limit':100}
        if cursor: query['page_token']=cursor
        page=api('GET','/api/2.2/jobs/list?'+urlencode(query))
        matches.extend(j for j in page.get('jobs',[]) if j.get('settings',{}).get('name')==JOB_NAME)
        cursor=page.get('next_page_token')
        if not cursor: break
    if len(matches)>1:
        raise RuntimeError('Multiple jobs have this name. Resolve the duplicates before continuing.')
    return matches[0]['job_id'] if matches else None

def deploy():
    notebooks=sorted((ROOT/'notebooks').glob('*.py'))
    if {p.stem for p in notebooks} != {'00_advanced_common','10_advanced_demo'}:
        raise RuntimeError('Expected exactly the two extension notebooks.')
    for p in notebooks: ast.parse(p.read_text())
    api('POST','/api/2.0/workspace/mkdirs',{'path':FOLDER})
    for p in notebooks:
        path=FOLDER+'/'+p.stem
        existing=api('GET','/api/2.0/workspace/get-status?'+urlencode({'path':path}),allow_missing=True)
        if existing:
            exported=api('GET','/api/2.0/workspace/export?'+urlencode({'path':path,'format':'SOURCE'}))
            remote=base64.b64decode(exported['content']).decode().strip()
            if remote != p.read_text().strip():
                raise RuntimeError(f'{path} already exists with different content. No overwrite performed.')
            print('Already imported:',p.stem,flush=True)
        else:
            api('POST','/api/2.0/workspace/import',{'path':path,'language':'PYTHON','format':'SOURCE',
                'content':base64.b64encode(p.read_bytes()).decode(),'overwrite':False})
            print('Imported:',p.stem,flush=True)
    job_id=find_job()
    if job_id is None:
        job_id=api('POST','/api/2.2/jobs/create',desired_job())['job_id']
    else:
        settings=api('GET',f'/api/2.2/jobs/get?job_id={job_id}')['settings']
        expected=desired_job()
        for key in ['timeout_seconds','max_concurrent_runs','performance_target']:
            if settings.get(key)!=expected[key]:
                raise RuntimeError('Existing job settings differ; inspect the job before running it.')
        ts=settings.get('tasks',[])
        if len(ts)!=1 or ts[0].get('notebook_task')!=expected['tasks'][0]['notebook_task']:
            raise RuntimeError('Existing job task differs; inspect the job before running it.')
        if settings.get('schedule') or settings.get('trigger') or settings.get('continuous'):
            raise RuntimeError('Existing job has an automatic trigger. Inspect it before proceeding.')
    print('Job:',job_id)
    print('Job page:',f'{HOST}/#job/{job_id}')
    return job_id

def status(run_id):
    run=api('GET',f'/api/2.2/jobs/runs/get?run_id={run_id}')
    print(json.dumps({'run_id':run_id,'state':run.get('state'),'status':run.get('status'),
        'tasks':[{'key':t['task_key'],'run_id':t.get('run_id'),'state':t.get('state')} for t in run.get('tasks',[])]},indent=2))
    for t in run.get('tasks',[]):
        if t.get('state',{}).get('result_state') in ['FAILED','TIMEDOUT']:
            output=api('GET',f"/api/2.2/jobs/runs/get-output?run_id={t['run_id']}")
            print(output.get('error',''))
            print(output.get('error_trace',''))

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--run',action='store_true',help='Deploy and submit a paid serverless run')
    parser.add_argument('--replay',action='store_true',help='With --run, submit one explicit replay after a successful initial extension run')
    parser.add_argument('--status',type=int,help='Read an existing run status; does not run compute')
    args=parser.parse_args()
    if args.status is not None:
        status(args.status); return
    if args.replay and not args.run:
        parser.error('--replay requires --run')
    job_id=deploy()
    if args.run:
        token=f'retail-advanced-{job_id}-'+('replay1' if args.replay else 'initial')
        run=api('POST','/api/2.2/jobs/run-now',{'job_id':job_id,'idempotency_token':token})
        print('Run submitted:',run['run_id'])
        print('Run page:',f"{HOST}/#job/{job_id}/run/{run['run_id']}")
    else:
        print('DEPLOYED. No run was submitted.')
        print('To start the demo: python3 azure-retail-advanced/deploy.py --run')

if __name__=='__main__':
    try: main()
    except (RuntimeError,KeyError,ValueError) as exc:
        print(str(exc),file=sys.stderr)
        sys.exit(1)
