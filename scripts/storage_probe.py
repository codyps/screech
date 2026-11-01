#!/usr/bin/env python3
"""Disposable sync-cost probes; does not alter service files or directory flags."""
import os,time,tempfile,statistics,json,sqlite3,fcntl,struct
from pathlib import Path
import argparse
p=argparse.ArgumentParser()
p.add_argument('--work-dir',type=Path,required=True,help='Existing directory on the filesystem being measured')
p.add_argument('--output',type=Path,required=True)
p.add_argument('--operations',type=int,default=1000)
args=p.parse_args();assert args.operations>0
out=[]
with tempfile.TemporaryDirectory(dir=args.work_dir,prefix='storage-probe-') as d:
    for nocow in [False,True]:
        for preallocate in [False,True]:
            for data_only in [False,True]:
                p=Path(d)/f'log-{nocow}-{preallocate}-{data_only}'
                fd=os.open(p,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
                if nocow:
                    try:
                        flags=struct.unpack('L',fcntl.ioctl(fd,0x80086601,struct.pack('L',0)))[0]
                        fcntl.ioctl(fd,0x40086602,struct.pack('L',flags|0x800000))
                    except OSError as e:
                        print('nocow unavailable',e);os.close(fd);continue
                if preallocate:
                    os.write(fd,b'\0'*(2*1024*1024));os.fsync(fd)
                samples=[]
                sync=os.fdatasync if data_only else os.fsync
                for i in range(args.operations):
                    t=time.perf_counter_ns();os.pwrite(fd,b'x'*512,i*512);sync(fd);samples.append((time.perf_counter_ns()-t)/1000)
                row=dict(nocow=nocow,preallocated=preallocate,sync='fdatasync' if data_only else 'fsync',p50_us=statistics.median(samples),p99_us=sorted(samples)[min(len(samples)-1,int(len(samples)*.99))]);out.append(row);print(row)
                os.close(fd)
    for nocow in [False,True]:
        directory=Path(d)/f'sqlite-{nocow}';directory.mkdir()
        if nocow:
            fd=os.open(directory,os.O_RDONLY)
            try:
                flags=struct.unpack('L',fcntl.ioctl(fd,0x80086601,struct.pack('L',0)))[0]
                fcntl.ioctl(fd,0x40086602,struct.pack('L',flags|0x800000))
            except OSError as error:
                print('nocow directory unavailable',error);os.close(fd);continue
            os.close(fd)
        c=sqlite3.connect(directory/'db');c.execute('pragma journal_mode=wal');c.execute('pragma synchronous=full');c.execute('create table meta(id integer primary key,n integer)');c.execute('insert into meta values(1,0)');c.execute('create table data(seq integer primary key,payload blob)');c.commit();samples=[]
        for i in range(args.operations):
            t=time.perf_counter_ns();c.execute('insert into data values(?,?)',(i,b'x'*200));c.execute('update meta set n=? where id=1',(i,));c.commit();samples.append((time.perf_counter_ns()-t)/1000)
        row=dict(sqlite=sqlite3.sqlite_version,nocow=nocow,p50_us=statistics.median(samples),p99_us=sorted(samples)[min(len(samples)-1,int(len(samples)*.99))]);out.append(row);print(row);c.close()
args.output.write_text(json.dumps(out,indent=2)+'\n')
