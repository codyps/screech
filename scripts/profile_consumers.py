#!/usr/bin/env python3
"""Profile checked consumers on a disposable local database (Linux perf or strace)."""
import argparse,os,select,signal,subprocess,tempfile
from pathlib import Path

def main(args):
    args.output.parent.mkdir(parents=True,exist_ok=True)
    env=os.environ.copy();env['TOKIO_WORKER_THREADS']='2'
    with tempfile.TemporaryDirectory(prefix='screech-profile-') as directory:
        command=(['perf','record','-e','cycles:u','-F','499','-o',str(args.output)+'.data','--'] if args.mode=='cpu' else
                 ['strace','-f','-c','-w','-e','trace=fsync,fdatasync,pwrite64,ftruncate','-o',str(args.output)+'.txt'])
        with open(str(args.output)+'.log','wb') as log:
            process=subprocess.Popen(command+[str(args.binary),'--port','0','--store',directory+'/data.db'],stdout=subprocess.PIPE,stderr=log,env=env,start_new_session=True)
            try:
                if not select.select([process.stdout],[],[],15)[0]:raise TimeoutError('profile startup')
                line=process.stdout.readline().decode().strip();assert line.startswith('LISTEN '),line
                result=subprocess.run([str(args.driver),'-url','nats://'+line.split()[1],'-scenario',args.scenario,'-n',str(args.messages),'-size','128','-subscribers','1'],check=True,capture_output=True,text=True,timeout=120)
                Path(str(args.output)+'-workload.json').write_text(result.stdout)
            finally:
                try:os.killpg(process.pid,signal.SIGINT)
                except ProcessLookupError:pass
                try:process.wait(timeout=10)
                except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
                process.stdout.close()
        if args.mode=='cpu':
            with open(str(args.output)+'.txt','w') as report:
                subprocess.run(['perf','report','-i',str(args.output)+'.data','--stdio','--no-children','--percent-limit','1'],check=True,stdout=report)
if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ['binary','driver']:p.add_argument('--'+key,type=lambda x:Path(x).resolve(),required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--mode',choices=['cpu','sync'],required=True)
    p.add_argument('--scenario',choices=['consume','consume-one','consume-parallel','consume-limits','consume-next','durable-parallel','consume-one-parallel','mixed-parallel','mixed-streams'],default='consume')
    p.add_argument('--messages',type=int,default=1000);args=p.parse_args();assert args.messages>0;main(args)
