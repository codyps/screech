#!/usr/bin/env python3
"""Reproducible checked workloads against both servers; raw JSON, no speed claims."""
from contextlib import contextmanager
from benchmark_cluster import BenchmarkCluster
import argparse
import json
import os
import re
import sys
import shutil
from datetime import datetime, timezone
from pathlib import Path
import platform
import socket
import statistics
import subprocess
import tempfile
import time
from wire import Client

def start(binary, label, durable, directory, payload_limit=0):
    s=socket.socket();s.bind(('127.0.0.1',0));port=s.getsockname()[1];s.close()
    address=('127.0.0.1',port)
    if label.startswith('screech'):
        cmd=[binary,'--port',str(port)]
        if payload_limit:
            # Large durable publications need one FULL commit per message; the default
            # 5 s storage deadline does not cover a 50 MiB write plus sync on slow disks.
            # Stored-message read-backs base64-expand payloads, so writer pending room
            # must exceed 4/3 of the largest payload.
            cmd+=['--max-payload',str(payload_limit),'--max-pending',str(payload_limit*3//2),'--storage-deadline-ms','30000']
        if durable:cmd+=['--store',str(directory/'screech.db')]
        if durable and label=='screech-unsafe-ack':cmd+=['--unsafe-ack-durability']
    else:
        config=directory/'nats.conf'
        config.write_text(f'host: 127.0.0.1\nport: {port}\n'+(f'max_payload: {payload_limit}\n' if payload_limit else '')+(f'jetstream {{\n store_dir: "{directory}/js"\n'+(' sync_interval: '+json.dumps(label.removeprefix('nats-sync-') if label.startswith('nats-sync-') else 'always')+'\n' if label!='nats-default' else '')+'}\n' if durable else ''))
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

@contextmanager
def running(binary, label, durable, directory, args, commands):
    if args.cluster_voters > 1:
        cluster = BenchmarkCluster(binary, label.startswith('screech'), args.cluster_voters, directory)
        commands.append(dict(server=label, durable=True, commands=cluster.commands, configs=cluster.configs))
        (args.output.parent/f'{args.output.stem}-{label}-launch.json').write_text(json.dumps(commands[-1],indent=2)+'\n')
        try:
            cluster.start()
            commands[-1]['readiness'] = cluster.readiness
            (args.output.parent/f'{args.output.stem}-{label}-launch.json').write_text(json.dumps(commands[-1],indent=2)+'\n')
            yield cluster.processes, cluster.port
            cluster.assert_alive()
        finally:
            cluster.close()
            cluster.save_logs(args.output, label)
    else:
        process, port, log, command = start(binary, label, durable, directory, LARGE_MESSAGE_PAYLOAD_LIMIT if args.large_messages else 0)
        commands.append(dict(server=label, durable=durable, command=command))
        try:
            yield [process], port
            if process.poll() is not None:
                raise RuntimeError('benchmark server exited')
        finally:
            process.kill();process.wait();log.close()
            shutil.copy2(directory/'server.log',args.output.parent/f'{args.output.stem}-{label}-{"durable" if durable else "core"}-server.log')


def total_usage(processes):
    values = [usage(process.pid) for process in processes]
    return {key: sum(v[key] for v in values) if all(v[key] is not None for v in values) else None
            for key in ('rss_kib', 'cpu_seconds')}

def filesystem_type(path):
    try:
        return subprocess.check_output(['stat','-f','-c','%T',str(path)],text=True,stderr=subprocess.DEVNULL).strip()
    except (OSError,subprocess.CalledProcessError):
        return None

def usage(pid):
    if not Path(f'/proc/{pid}/status').exists():return {'rss_kib':None,'cpu_seconds':None}
    rss=0
    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
        if line.startswith('VmRSS:'):rss=int(line.split()[1])
    fields=Path(f'/proc/{pid}/stat').read_text().split()
    return {'rss_kib':rss,'cpu_seconds':(int(fields[13])+int(fields[14]))/os.sysconf('SC_CLK_TCK')}

JETSTREAM_CASES = [('core-stream',128,1,5000),('durable',128,1,5000),('consume',128,1,1000),('consume-limits',128,1,1000),('consume-one',128,1,1000),('consume-one',1024,1,1000),('consume-next',128,1,1000),('consume-parallel',128,1,5000),('consume-ack-window',128,1,5000),('dedup',128,1,5000),('durable-parallel',128,1,5000),('consume-one-parallel',128,1,5000),('mixed-parallel',128,1,5000),('mixed-streams',128,1,5000)]
CONCURRENT_CASES = [(name,128,1,20000) for name in ('durable-parallel','consume-one-parallel','mixed-parallel','mixed-streams')] + [('consume-limits',128,1,10000)]
# Durable publications of 1 MiB, 10 MiB and 50 MiB payloads, keeping each trial's
# transferred bytes near 100-200 MiB so FULL-sync commits stay the dominant cost.
# Both servers keep a 1 MiB default protocol payload limit; large-message trials
# raise it once, with headroom over the largest payload for control-line overhead.
LARGE_MESSAGE_PAYLOAD_LIMIT = 64<<20
# nats-server's file store rejects a message record above 32 MiB outright
# (ErrMsgTooLarge, err_code 10077): record length counts 30 bytes of overhead plus
# the subject and any headers, and no config raises it (max_payload only widens the
# protocol limit, stream max_msg_size can only lower it, s2 compression runs after
# the check, and the same check gates clustered R3/R5 proposals). Its modes cannot
# join cases at or near that boundary; Screech modes can. The 64-byte allowance
# covers the benchmark subject plus record overhead.
NATS_FILE_RECORD_LIMIT = 32<<20
NATS_FILE_RECORD_SLACK = 64
# The largest payload nats-server's file store can record for the benchmark subject
# ('bench.' + a 19-digit nanosecond timestamp = 25 bytes + 30 overhead <= the slack):
# the biggest message that still runs against every server mode.
NATS_MAX_FILE_PAYLOAD = NATS_FILE_RECORD_LIMIT - NATS_FILE_RECORD_SLACK
# The fourth size exceeds the NATS file-store record cap and runs Screech modes only.
LARGE_MESSAGE_CASES = [('durable',1<<20,1,100),('durable',10<<20,1,10),('durable',NATS_MAX_FILE_PAYLOAD,1,4),('durable',50<<20,1,4)]
# Durable warmups default to 20 messages; bound them by transferred bytes instead.
WARMUP_BYTE_BUDGET = 128<<20

def main(args):
    rows=[];commands=[]
    if not 1 <= args.trials <= 10: raise ValueError('trials must be 1..10')
    for interval in args.nats_sync_interval:
        if not re.fullmatch(r'(?:[0-9]+(?:\.[0-9]+)?(?:ns|us|ms|s|m|h))+',interval): raise ValueError('invalid NATS sync interval')
    if len(set(args.nats_sync_interval)) != len(args.nats_sync_interval): raise ValueError('duplicate sync intervals')
    if args.cluster_messages <= 0: raise ValueError('cluster messages must be positive')
    if args.cluster_voters > 1 and (args.screech_unsafe_ack or args.screech_before or args.nats_default or args.nats_sync_interval):
        raise ValueError('cluster campaigns use strict Screech and NATS always only')
    if args.large_messages and args.cluster_voters > 1:
        raise ValueError('large-message campaigns are standalone only')
    if args.large_messages and args.scale_only:
        raise ValueError('select one focused campaign: --scale-only or --large-messages')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix('.jsonl').write_text('')
    core=[('fanout',8,1,2000000),('wildcard',128,1,2000000),('fanout',128,1,2000000),('fanout',1024,1,1000000),('fanout',128,4,1000000),('queue',128,4,2000000),('request',128,1,50000),('core-subject-churn',128,1,1000000),('core-many-subscriptions',128,1,1000000),('core-churn-many-subscriptions',128,1,1000000)]
    for durable in (False,True):
        if args.cluster_voters > 1 and not durable: continue
        cases=JETSTREAM_CASES if durable else core
        if args.scale_only:
            if not durable: continue
            cases=CONCURRENT_CASES
        if args.large_messages:
            if not durable: continue
            cases=LARGE_MESSAGE_CASES
        if args.scenario: cases=[case for case in cases if case[0] in args.scenario]
        if args.cluster_voters > 1:
            cases=[(scenario,size,subs,args.cluster_messages) for scenario,size,subs,n in cases]
        if not cases: continue
        # Alternate server order each invocation with --reverse for larger campaigns.
        servers=[('screech',args.screech),('nats-main',args.nats)]
        if durable and args.nats_default:servers.append(('nats-default',args.nats))
        if durable: servers.extend(('nats-sync-'+interval,args.nats) for interval in args.nats_sync_interval)
        if durable and args.screech_before:servers.append(('screech-before',args.screech_before))
        if durable and args.screech_unsafe_ack:servers.append(('screech-unsafe-ack',args.screech))
        if args.cluster_voters > 1:
            servers=[(f'screech-cluster-{args.cluster_voters}',args.screech), (f'nats-cluster-{args.cluster_voters}-always',args.nats)]
        if args.reverse:servers.reverse()
        for label,binary in servers:
            with tempfile.TemporaryDirectory(prefix='screech-bench-') as tmp:
                directory=Path(tmp)
                with running(binary,label,durable,directory,args,commands) as (processes,port):
                    for scenario,size,subs,n in cases:
                        if args.large_messages and not label.startswith('screech') and size > NATS_FILE_RECORD_LIMIT - NATS_FILE_RECORD_SLACK:
                            print(f"Skipped {label} {scenario} size={size}: nats-server file storage cannot represent message records above 32 MiB",file=sys.stderr)
                            continue
                        base=[args.driver,'-url',f'nats://127.0.0.1:{port}','-scenario',scenario,'-size',str(size),'-subscribers',str(subs),'-replicas',str(args.cluster_voters)]
                        # Durable warmups move a bounded number of bytes: 20 small messages,
                        # but only 2 x 50 MiB publications for the largest payload.
                        warmup=1000 if not durable else max(1,min(20,WARMUP_BYTE_BUDGET//size))
                        try:
                            subprocess.run(base+['-n',str(warmup)],check=True,text=True,timeout=60,stdout=subprocess.DEVNULL)
                            for trial in range(args.trials):
                                before=total_usage(processes)
                                result=subprocess.run(base+['-n',str(n)],check=True,capture_output=True,text=True,timeout=180 if args.cluster_voters > 1 else 300 if args.large_messages else 60)
                                after=total_usage(processes)
                                row=json.loads(result.stdout)
                                if row.get('checked') is not True or row.get('stream_replicas') != args.cluster_voters:
                                    raise ValueError('unchecked or incorrectly replicated workload')
                                row.update(server=label,trial=trial,voters=args.cluster_voters,server_rss_kib=after['rss_kib'],server_cpu_seconds=(after['cpu_seconds']-before['cpu_seconds']) if after['cpu_seconds'] is not None and before['cpu_seconds'] is not None else None)
                                rows.append(row);print(json.dumps(row),flush=True)
                                with args.output.with_suffix('.jsonl').open('a') as journal:
                                    journal.write(json.dumps(row)+'\n')
                        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                            print(f"Checked workload failed: {label} {scenario} size={size} subscribers={subs}",file=sys.stderr)
                            if error.stdout:print(error.stdout,file=sys.stderr)
                            if error.stderr:print(error.stderr,file=sys.stderr)
                            raise
    cpu=next((x.split(':',1)[1].strip() for x in Path('/proc/cpuinfo').read_text().splitlines() if x.startswith('model name')),platform.processor())
    output={'environment':{'recorded_at':datetime.now(timezone.utc).isoformat(),'revision':os.environ.get('GITHUB_SHA'),'ci_run_id':os.environ.get('GITHUB_RUN_ID'),'ci_run_attempt':os.environ.get('GITHUB_RUN_ATTEMPT'),'nats_sync_intervals':['always']+(['default 2m'] if args.nats_default else [])+args.nats_sync_interval,'kernel':platform.release(),'cpu':cpu,'logical_cpus':os.cpu_count(),'store_filesystem':filesystem_type(tempfile.gettempdir()),'server_workers_per_voter':2,'voters':args.cluster_voters,'resource_scope':'sum of all voter processes','transport':'loopback TCP; voters share one host and filesystem' if args.cluster_voters > 1 else 'loopback TCP','durability':'SQLite WAL FULL by default; screech-unsafe-ack buffers consumer state for a 50ms flush target, publication ACKs remain FULL; nats-main sync_interval always; optional nats-default uses default 2m','cluster_durability':'Screech synced accepted-journal quorum and FULL checkpoint roots; NATS replicated file streams/consumers with sync_interval always; persistence contracts differ' if args.cluster_voters > 1 else None,'nats_commit':args.nats_commit,'driver_binary_sha256':__import__('hashlib').sha256(Path(args.driver).read_bytes()).hexdigest(),'screech_binary_sha256':__import__('hashlib').sha256(Path(args.screech).read_bytes()).hexdigest(),'screech_before_binary_sha256':__import__('hashlib').sha256(Path(args.screech_before).read_bytes()).hexdigest() if args.screech_before else None,'nats_binary_sha256':__import__('hashlib').sha256(Path(args.nats).read_bytes()).hexdigest(),'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),'nats_version':subprocess.check_output([args.nats,'--version'],text=True).strip(),'concurrent_test_workloads':False,'large_message_workloads':args.large_messages,'max_payload_bytes':LARGE_MESSAGE_PAYLOAD_LIMIT if args.large_messages else 1048576,'large_message_nats_skip':'nats-server file storage caps message records at 32 MiB; the 50 MiB case runs Screech modes only' if args.large_messages else None},'commands':commands,'rows':rows}
    args.output.write_text(json.dumps(output,indent=2)+'\n')
if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ('screech','nats','driver'):p.add_argument('--'+key,required=True,type=lambda x:str(Path(x).resolve()))
    p.add_argument('--screech-unsafe-ack',action='store_true',help='Also benchmark explicitly unsafe consumer ACK durability');p.add_argument('--nats-sync-interval',action='append',default=[],help='Additional NATS stream sync interval, e.g. 10ms (repeatable)');p.add_argument('--scale-only',action='store_true');p.add_argument('--large-messages',action='store_true',help='Run 1 MiB/10 MiB/~32 MiB/50 MiB durable publication cases with a raised payload limit (standalone only)');p.add_argument('--screech-before',type=lambda x:str(Path(x).resolve()));p.add_argument('--nats-default',action='store_true');p.add_argument('--nats-commit',required=True);p.add_argument('--trials',type=int,default=3);p.add_argument('--reverse',action='store_true');p.add_argument('--output',type=Path,required=True)
    p.add_argument('--cluster-voters',type=int,choices=(1,3,5),default=1,help='Separate strict clustered campaign; skips transient Core cases')
    p.add_argument('--cluster-messages',type=int,default=1000,help='Publications per clustered trial (default: 1000)')
    p.add_argument('--scenario',action='append',choices=sorted(set(c[0] for c in JETSTREAM_CASES)),help='Run selected JetStream cases only (repeatable)')
    main(p.parse_args())
