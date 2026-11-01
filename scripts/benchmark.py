#!/usr/bin/env python3
"""Reproducible checked workloads against both servers; raw JSON, no speed claims."""
import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
import platform
import socket
import statistics
import subprocess
import tempfile
import time
from wire import Client

def start(binary, label, durable, directory):
    s=socket.socket();s.bind(('127.0.0.1',0));port=s.getsockname()[1];s.close()
    address=('127.0.0.1',port)
    if label.startswith('screech'):
        cmd=[binary,'--port',str(port)]
        if durable:cmd+=['--store',str(directory/'screech.db')]
    else:
        config=directory/'nats.conf'
        config.write_text(f'host: 127.0.0.1\nport: {port}\n'+(f'jetstream {{\n store_dir: "{directory}/js"\n'+(' sync_interval: '+json.dumps(label.removeprefix('nats-sync-') if label.startswith('nats-sync-') else 'always')+'\n' if label!='nats-default' else '')+'}\n' if durable else ''))
        cmd=[binary,'-c',str(config)]
    env=os.environ.copy();env.update(TOKIO_WORKER_THREADS='2',GOMAXPROCS='2')
    log=open(directory/'server.log','wb');process=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT,env=env)
    deadline=time.monotonic()+10
    try:
        while True:
            if process.poll() is not None:raise RuntimeError((directory/'server.log').read_text())
            try:c=Client(address);c.close();return process,port,log,cmd
            except OSError:
                if time.monotonic()>deadline:raise TimeoutError('server startup')
                time.sleep(.01)
    except BaseException:
        process.kill();process.wait();log.close();raise

def usage(pid):
    if not Path(f'/proc/{pid}/status').exists():return {'rss_kib':None,'cpu_seconds':None}
    rss=0
    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
        if line.startswith('VmRSS:'):rss=int(line.split()[1])
    fields=Path(f'/proc/{pid}/stat').read_text().split()
    return {'rss_kib':rss,'cpu_seconds':(int(fields[13])+int(fields[14]))/os.sysconf('SC_CLK_TCK')}

def main(args):
    rows=[];commands=[]
    if not 1 <= args.trials <= 10: raise ValueError('trials must be 1..10')
    for interval in args.nats_sync_interval:
        if not re.fullmatch(r'(?:[0-9]+(?:\.[0-9]+)?(?:ns|us|ms|s|m|h))+',interval): raise ValueError('invalid NATS sync interval')
    if len(set(args.nats_sync_interval)) != len(args.nats_sync_interval): raise ValueError('duplicate sync intervals')
    args.output.with_suffix('.jsonl').write_text('')
    core=[('fanout',8,1,2000000),('wildcard',128,1,2000000),('fanout',128,1,2000000),('fanout',1024,1,1000000),('fanout',128,4,1000000),('queue',128,4,2000000),('request',128,1,50000)]
    for durable in (False,True):
        cases=[('core-stream',128,1,5000),('durable',128,1,5000),('consume',128,1,1000),('consume-limits',128,1,1000),('consume-one',128,1,1000),('consume-one',1024,1,1000),('consume-next',128,1,1000),('consume-parallel',128,1,5000),('consume-ack-window',128,1,5000),('dedup',128,1,5000),('durable-parallel',128,1,5000),('consume-one-parallel',128,1,5000),('mixed-parallel',128,1,5000),('mixed-streams',128,1,5000)] if durable else core
        if args.scale_only:
            if not durable: continue
            cases=[(name,128,1,20000) for name in ('durable-parallel','consume-one-parallel','mixed-parallel','mixed-streams')]
        # Alternate server order each invocation with --reverse for larger campaigns.
        servers=[('screech',args.screech),('nats-main',args.nats)]
        if args.reverse:servers.reverse()
        if durable and args.nats_default:servers.append(('nats-default',args.nats))
        if durable: servers.extend(('nats-sync-'+interval,args.nats) for interval in args.nats_sync_interval)
        if durable and args.screech_before:servers.append(('screech-before',args.screech_before))
        for label,binary in servers:
            with tempfile.TemporaryDirectory(prefix='screech-bench-') as tmp:
                directory=Path(tmp);process,port,log,cmd=start(binary,label,durable,directory);commands.append({'server':label,'durable':durable,'command':cmd})
                try:
                    for scenario,size,subs,n in cases:
                        base=[args.driver,'-url',f'nats://127.0.0.1:{port}','-scenario',scenario,'-size',str(size),'-subscribers',str(subs)]
                        subprocess.run(base+['-n','1000' if not durable else '20'],check=True,text=True,timeout=45,stdout=subprocess.DEVNULL)
                        for trial in range(args.trials):
                            before=usage(process.pid)
                            result=subprocess.run(base+['-n',str(n)],check=True,capture_output=True,text=True,timeout=60)
                            after=usage(process.pid)
                            row=json.loads(result.stdout);row.update(server=label,trial=trial,server_rss_kib=after['rss_kib'],server_cpu_seconds=(after['cpu_seconds']-before['cpu_seconds']) if after['cpu_seconds'] is not None and before['cpu_seconds'] is not None else None)
                            rows.append(row);print(json.dumps(row),flush=True)
                            args.output.with_suffix('.jsonl').open('a').write(json.dumps(row)+'\n')
                finally:
                    process.kill();process.wait();log.close()
    cpu=next((x.split(':',1)[1].strip() for x in Path('/proc/cpuinfo').read_text().splitlines() if x.startswith('model name')),platform.processor())
    output={'environment':{'recorded_at':datetime.now(timezone.utc).isoformat(),'nats_sync_intervals':['always']+(['default 2m'] if args.nats_default else [])+args.nats_sync_interval,'kernel':platform.release(),'cpu':cpu,'logical_cpus':os.cpu_count(),'server_workers':2,'transport':'loopback TCP','durability':'SQLite WAL FULL; nats-main sync_interval always; optional nats-default uses default 2m','nats_commit':args.nats_commit,'screech_binary_sha256':__import__('hashlib').sha256(Path(args.screech).read_bytes()).hexdigest(),'screech_before_binary_sha256':__import__('hashlib').sha256(Path(args.screech_before).read_bytes()).hexdigest() if args.screech_before else None,'nats_binary_sha256':__import__('hashlib').sha256(Path(args.nats).read_bytes()).hexdigest(),'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),'nats_version':subprocess.check_output([args.nats,'--version'],text=True).strip(),'concurrent_test_workloads':False},'commands':commands,'rows':rows}
    args.output.write_text(json.dumps(output,indent=2)+'\n')
if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ('screech','nats','driver'):p.add_argument('--'+key,required=True,type=lambda x:str(Path(x).resolve()))
    p.add_argument('--nats-sync-interval',action='append',default=[],help='Additional NATS stream sync interval, e.g. 10ms (repeatable)');p.add_argument('--scale-only',action='store_true');p.add_argument('--screech-before',type=lambda x:str(Path(x).resolve()));p.add_argument('--nats-default',action='store_true');p.add_argument('--nats-commit',required=True);p.add_argument('--trials',type=int,default=3);p.add_argument('--reverse',action='store_true');p.add_argument('--output',type=Path,required=True)
    main(p.parse_args())
