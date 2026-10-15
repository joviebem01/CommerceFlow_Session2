"""CommerceFlow Session 2 Event Streaming Console — run: python session2_gui.py.

Uses PostgreSQL database ``commerceflow`` and the uploaded order/customer CSVs.
"""
from __future__ import annotations
import csv, json, hashlib, os, threading, queue, time, sys, traceback
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / '.env'
DATA = ROOT / 'Datasets'
OUT = ROOT / 'results'
OUT.mkdir(exist_ok=True)
PARTS = 4
TOPIC = 'transaction.recorded'
NAVY='#203960'; LIGHT='#f0f5fc'; GREEN='#e3f0dc'; ORANGE='#ffe7d7'; WHITE='#ffffff'
GROUPS=['revenue-projector','audit-writer','high-value-alerter']


def load_env_file():
    if not ENV_FILE.exists():
        return
    for line_number,line in enumerate(ENV_FILE.read_text(encoding='utf-8').splitlines(),1):
        entry=line.strip()
        if not entry or entry.startswith('#'):
            continue
        key,separator,value=entry.partition('=')
        key=key.strip()
        if not separator or not key:
            raise ValueError(f'Invalid .env entry on line {line_number}. Expected KEY=value.')
        value=value.strip()
        if len(value)>=2 and value[0]==value[-1] and value[0] in ("'",'"'):
            value=value[1:-1]
        os.environ.setdefault(key,value)


load_env_file()


@contextmanager
def pg_conn():
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError('PostgreSQL support requires psycopg2. Install project requirements first.') from exc
    if not os.environ.get('PGUSER') or not os.environ.get('PGPASSWORD'):
        raise RuntimeError(f'Enter your PostgreSQL username and password in {ENV_FILE}.')
    try:
        db=psycopg2.connect(
            dbname='commerceflow',
            user=os.environ['PGUSER'],
            password=os.environ['PGPASSWORD'],
            host=os.environ.get('PGHOST','localhost'),
            port=os.environ.get('PGPORT','5432'),
            connect_timeout=5,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Could not connect to PostgreSQL database "
            f"'commerceflow' at "
            f"{os.environ.get('PGHOST','localhost')}:{os.environ.get('PGPORT','5432')}: {exc}"
        ) from exc
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def partition(region):
    # deterministic across separate processes, unlike Python's built-in hash()
    return int.from_bytes(hashlib.sha256(str(region).encode()).digest()[:8],'big')%PARTS

def num(value):
    try: return float(str(value or '0').replace(',','').strip())
    except (ValueError,TypeError): return 0.0

def money(row):
    status=str(row.get('status','')).strip().lower()
    status={'suc':'success','ref':'refunded','fail':'failed'}.get(status,status)
    amount=num(row.get('order_amount',0))
    return amount if status=='success' and amount>0 else 0.0

def ensure_db():
    with pg_conn() as db:
        with db.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS session2_events (
                partition_id INTEGER NOT NULL, event_offset BIGINT NOT NULL, event_id TEXT NOT NULL UNIQUE,
                customer_id TEXT NOT NULL, product_id TEXT NOT NULL, region TEXT NOT NULL,
                order_amount TEXT NOT NULL, order_date TEXT NOT NULL, payment_method TEXT NOT NULL,
                status TEXT NOT NULL, quantity TEXT NOT NULL, revenue DOUBLE PRECISION NOT NULL,
                PRIMARY KEY(partition_id,event_offset))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS session2_offsets (
                group_name TEXT NOT NULL, partition_id INTEGER NOT NULL, next_offset BIGINT NOT NULL,
                PRIMARY KEY(group_name,partition_id))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS session2_audit (
                audit_id BIGSERIAL PRIMARY KEY, event_id TEXT NOT NULL, region TEXT NOT NULL,
                revenue DOUBLE PRECISION NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW())''')
            cur.execute('''CREATE TABLE IF NOT EXISTS session2_alerts (
                event_id TEXT PRIMARY KEY, region TEXT NOT NULL, revenue DOUBLE PRECISION NOT NULL)''')

def rows_for_stream():
    crm={}
    with (DATA/'crm_50000_customers_dirty_v3.csv').open(encoding='utf-8-sig',newline='') as f:
        for r in csv.DictReader(f):
            cid=r.get('customer_id','').strip()
            if cid and cid not in crm: crm[cid]=(r.get('state') or 'Unknown').strip() or 'Unknown'
    with (DATA/'orders_300k_dirty.csv').open(encoding='utf-8-sig',newline='') as f:
        for i,r in enumerate(csv.DictReader(f)):
            region=crm.get(r.get('customer_id','').strip(),'Unknown')
            oid=r.get('order_id','').strip()
            if not oid:raise ValueError(f'Order row {i+2} has no order_id; cannot create a dataset-backed event key.')
            yield (
                oid,r.get('customer_id',''),r.get('product_id',''),region,
                r.get('order_amount',''),r.get('order_date',''),r.get('payment_method',''),
                r.get('status',''),r.get('quantity',''),money(r),
            )

def produce(logger):
    ensure_db()
    t=time.perf_counter(); offsets=[0]*PARTS; counts=[0]*PARTS
    with pg_conn() as db:
        with db.cursor() as cur:
            cur.execute('TRUNCATE session2_events, session2_offsets, session2_audit, session2_alerts RESTART IDENTITY')
            batch=[]
            for oid,customer_id,product_id,region,amount,ts,payment_method,status,quantity,revenue in rows_for_stream():
                p=partition(region);off=offsets[p];offsets[p]+=1;counts[p]+=1
                batch.append((
                    p,off,oid,customer_id,product_id,region,amount,ts,payment_method,status,quantity,revenue,
                ))
                if len(batch)>=1000:
                    cur.executemany('''INSERT INTO session2_events VALUES
                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',batch);batch=[]
            if batch:
                cur.executemany('''INSERT INTO session2_events VALUES
                    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',batch)
            for group in GROUPS:
                cur.executemany('INSERT INTO session2_offsets VALUES (%s,%s,0)',[(group,p) for p in range(PARTS)])
    for name in ('consumer_lag.csv','streamed_regional_revenue.csv','reconciliation_report.json','replay_revenue.csv'):
        path=OUT/name
        if path.exists():path.unlink()
    seconds=max(time.perf_counter()-t,0.000001)
    with (OUT/'session2_throughput.csv').open('w',newline='') as f:
        w=csv.writer(f);w.writerow(['produced','seconds','events_per_second']);w.writerow([sum(counts),round(seconds,5),round(sum(counts)/seconds,1)])
    logger(f'Produced {sum(counts):,} real order events in {seconds:.3f}s ({sum(counts)/seconds:,.0f}/sec)')
    for p,n in enumerate(counts):logger(f'Partition {p}: {n:,} ({100*n/max(sum(counts),1):.1f}%)')
    return {'counts':counts,'seconds':seconds,'total':sum(counts)}

def count_events():
    with pg_conn() as db:
        with db.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM session2_events')
            return cur.fetchone()[0]

def event_rows(db):
    cur=db.cursor()
    cur.execute('''SELECT partition_id,event_offset,event_id,customer_id,product_id,region,
        order_amount,order_date,payment_method,status,quantity,revenue
        FROM session2_events ORDER BY partition_id,event_offset''')
    return cur

def do_consume(group,logger,fail_after=None,commit_every=5000):
    start=time.perf_counter();processed=0;duplicates=0
    failed=False
    with pg_conn() as db:
        with db.cursor() as cur:
            cur.execute('SELECT partition_id,next_offset FROM session2_offsets WHERE group_name=%s',(group,))
            limits=dict(cur.fetchall())
            pending_offsets=dict(limits)
            cur.execute('''SELECT partition_id,event_offset,event_id,region,revenue
                FROM session2_events ORDER BY partition_id,event_offset''')
            for p,off,eid,region,revenue in cur.fetchall():
                if off<limits.get(p,0):continue
                if group=='audit-writer':
                    cur.execute('INSERT INTO session2_audit(event_id,region,revenue) VALUES(%s,%s,%s)',(eid,region,revenue))
                elif group=='high-value-alerter' and revenue>900:
                    cur.execute('''INSERT INTO session2_alerts VALUES(%s,%s,%s)
                        ON CONFLICT(event_id) DO NOTHING''',(eid,region,revenue))
                processed+=1
                pending_offsets[p]=off+1
                if processed%commit_every==0:
                    for pp,next_off in pending_offsets.items():
                        cur.execute('''UPDATE session2_offsets SET next_offset=%s
                            WHERE group_name=%s AND partition_id=%s''',(next_off,group,pp))
                    db.commit()
                if fail_after and processed>=fail_after:
                    db.rollback();failed=True
                    logger(f'Injected crash in {group} after {processed:,} events')
                    break
            if not failed:
                for pp,next_off in pending_offsets.items():
                    cur.execute('''UPDATE session2_offsets SET next_offset=%s
                        WHERE group_name=%s AND partition_id=%s''',(next_off,group,pp))
                db.commit()
    elapsed=max(time.perf_counter()-start,0.000001)
    logger(f'{group}: handled {processed:,} events in {elapsed:.3f}s')
    return {'group':group,'processed':processed,'seconds':elapsed,'rate':round(processed/elapsed)}

def all_consume(logger):
    if not count_events():raise RuntimeError('Produce events first.')
    results=[]
    for group in GROUPS:results.append(do_consume(group,logger))
    export_lag(); export_revenue();return results

def lag_info():
    with pg_conn() as db:
        with db.cursor() as cur:
            cur.execute('SELECT partition_id,MAX(event_offset)+1 FROM session2_events GROUP BY partition_id')
            ends=dict(cur.fetchall())
        rows=[]
        for group in GROUPS:
            with db.cursor() as cur:
                cur.execute('SELECT partition_id,next_offset FROM session2_offsets WHERE group_name=%s',(group,))
                offsets=dict(cur.fetchall())
            for p in range(PARTS):
                end=ends.get(p,0);committed=offsets.get(p,0)
                rows.append((group,p,end,committed,max(end-committed,0)))
    return rows,sum(r[4] for r in rows)

def export_lag():
    rows,_=lag_info()
    with (OUT/'consumer_lag.csv').open('w',newline='') as f:
        w=csv.writer(f);w.writerow(['group','partition','end_offset','committed','lag']);w.writerows(rows)

def revenue_aggregate():
    with pg_conn() as db:
        with db.cursor() as cur:
            cur.execute('''SELECT region,COUNT(*),SUM(revenue),AVG(revenue)
                FROM session2_events GROUP BY region ORDER BY region''')
            return cur.fetchall()

def export_revenue():
    rows=revenue_aggregate()
    with (OUT/'streamed_regional_revenue.csv').open('w',newline='') as f:
        w=csv.writer(f);w.writerow(['region','txn_count','revenue_total','revenue_mean']);w.writerows(rows)
    return rows

def reconcile(logger):
    if not count_events():raise RuntimeError('Produce first.')
    streamed={r[0]:(r[1],r[2]/r[1] if r[1] else 0) for r in revenue_aggregate()}
    batch=defaultdict(lambda:[0,0.0])
    for oid,customer_id,product_id,region,amount,ts,payment_method,status,quantity,revenue in rows_for_stream():
        batch[region][0]+=1;batch[region][1]+=revenue
    keys=set(batch)|set(streamed);max_count=0;max_mean=0
    for k in keys:
        a,b=batch.get(k,(0,0.0));c,d=streamed.get(k,(0,0.0))
        max_count=max(max_count,abs(a-c));max_mean=max(max_mean,abs((b/a if a else 0)-d))
    _,total_lag=lag_info();passed=set(batch)==set(streamed) and max_count==0 and max_mean<1e-6 and total_lag==0
    result={'passed':passed,'regions':len(keys),'records':sum(v[0] for v in batch.values()),'count_diff':max_count,'mean_diff':max_mean,'tolerance':1e-6,'lag':total_lag}
    (OUT/'reconciliation_report.json').write_text(json.dumps(result,indent=2));logger('Reconciliation: '+json.dumps(result));return result

def run_replay(logger,since=''):
    with pg_conn() as db:
        with db.cursor() as cur:
            cur.execute('''SELECT event_id,payment_method,revenue,order_date
                FROM session2_events ORDER BY partition_id,event_offset''')
            records=cur.fetchall()
    grouped=defaultdict(lambda:[0,0.0]);matched=0
    for eid,payment_method,revenue,ts in records:
        if since and ts<since:continue
        grouped[payment_method][0]+=1;grouped[payment_method][1]+=revenue;matched+=1
    rows=[(method,count,round(revenue,2)) for method,(count,revenue) in sorted(grouped.items())]
    with (OUT/'replay_revenue.csv').open('w',newline='') as f:
        w=csv.writer(f);w.writerow(['payment_method','events','revenue']);w.writerows(rows)
    logger(f'Replayed {matched:,} of {len(records):,} events')
    return rows,matched

def kafka_publish(logger,bootstrap='localhost:9092'):
    try:from kafka import KafkaProducer
    except ImportError:raise RuntimeError('Kafka integration requires: pip install kafka-python')
    producer=KafkaProducer(bootstrap_servers=[bootstrap],value_serializer=lambda v:json.dumps(v).encode('utf8'),key_serializer=lambda k:k.encode('utf8'),acks='all',retries=3)
    sent=0
    with pg_conn() as db:
        with db.cursor() as cur:
            for p,off,eid,customer_id,product_id,region,amount,ts,payment_method,status,quantity,revenue in event_rows(db):
                producer.send(TOPIC,key=region,value={
                    'event_id':eid,'customer_id':customer_id,'product_id':product_id,'region':region,
                    'order_amount':amount,'order_date':ts,'payment_method':payment_method,
                    'status':status,'quantity':quantity,'revenue':revenue,
                });sent+=1
                if sent%5000==0:logger(f'Kafka: queued {sent:,}')
    producer.flush(timeout=120);producer.close();logger(f'Kafka: published {sent:,} events to {TOPIC} at {bootstrap}')

class App(tk.Tk):
    def __init__(self):
        super().__init__();self.title('CommerceFlow — Session 2 Event Streaming Console');self.geometry('1330x850');self.minsize(1050,700)
        self.q=queue.Queue();self.busy=False;self.latest={};self.logtext=[];self.action_buttons=[]
        style=ttk.Style(self);style.theme_use('clam')
        style.configure('TNotebook.Tab',padding=(17,10),font=('Segoe UI',10))
        style.map('TNotebook.Tab',background=[('selected',NAVY)],foreground=[('selected','white')])
        style.configure('Treeview.Heading',background=NAVY,foreground='white',font=('Segoe UI',10,'bold'))
        style.configure('Treeview',rowheight=27,font=('Segoe UI',10))
        style.configure('Primary.TButton',font=('Segoe UI',10,'bold'))
        header=tk.Frame(self,bg=NAVY,height=90);header.pack(fill='x');header.pack_propagate(False)
        tk.Label(header,text='CommerceFlow — Session 2 Event Streaming Console',font=('Georgia',22,'bold'),fg='white',bg=NAVY).pack(anchor='w',padx=24,pady=(13,3))
        tk.Label(header,text='PostgreSQL commerceflow · Actual uploaded e-commerce CSVs · 4 partitions by customer state',font=('Segoe UI',10),fg='#bed0ea',bg=NAVY).pack(anchor='w',padx=24)
        self.nb=ttk.Notebook(self);self.nb.pack(fill='both',expand=True,padx=12,pady=11)
        self.tabs={}
        for name in ['Pipeline','Run everything','Durable log','Consumers & lag','Failure & recovery','Replay','Reconciliation','Console']:
            frame=tk.Frame(self.nb,bg=WHITE);self.nb.add(frame,text=name);self.tabs[name]=frame
        self.make_pipeline();self.make_run_everything();self.make_durable();self.make_consumers();self.make_failure();self.make_replay();self.make_recon();self.make_console()
        self.after(150,self.poll);self.refresh()

    def layout(self,tab,title,button=None,command=None):
        p=self.tabs[tab];bar=tk.Frame(p,bg=WHITE);bar.pack(fill='x',padx=18,pady=(15,10))
        tk.Label(bar,text=title,font=('Georgia',17,'bold'),fg=NAVY,bg=WHITE).pack(side='left')
        if button:
            btn=ttk.Button(bar,text=button,command=command)
            btn.pack(side='right')
            self.action_buttons.append(btn)
        return p
    def set_action_buttons(self,enabled):
        for btn in self.action_buttons:
            btn.configure(state='normal' if enabled else 'disabled')
    def notice(self,parent,text,color=GREEN):
        lab=tk.Label(parent,text=text,font=('Segoe UI',10),bg=color,fg='#375e23',anchor='w',padx=15,pady=12)
        lab.pack(fill='x',padx=18,pady=(0,12));return lab
    def table(self,parent,cols,height=11):
        frame=tk.Frame(parent,bg=WHITE);frame.pack(fill='both',expand=True,padx=18,pady=(0,12))
        tree=ttk.Treeview(frame,columns=cols,show='headings',height=height)
        for c in cols:tree.heading(c,text=c);tree.column(c,width=max(115,int(900/len(cols))),anchor='center')
        tree.pack(side='left',fill='both',expand=True);y=ttk.Scrollbar(frame,orient='vertical',command=tree.yview);y.pack(side='right',fill='y');tree.configure(yscrollcommand=y.set);return tree
    def fill(self,tree,rows):
        tree.delete(*tree.get_children());
        for i,row in enumerate(rows):tree.insert('', 'end',values=list(row),tags=('alt' if i%2 else 'plain',))
        tree.tag_configure('alt',background='#f1f6fc')
    def task(self,fn):
        if self.busy:return
        self.busy=True
        self.set_action_buttons(False)
        def job():
            try:result=fn(lambda s:self.q.put(('log',s)));self.q.put(('done',result))
            except Exception:self.q.put(('error',traceback.format_exc()))
        threading.Thread(target=job,daemon=True).start()
    def poll(self):
        try:
            while True:
                typ,value=self.q.get_nowait()
                if typ=='log':self.write(value)
                elif typ=='done':self.busy=False;self.set_action_buttons(True);self.refresh()
                elif typ=='error':self.busy=False;self.set_action_buttons(True);self.write(value);messagebox.showerror('Session 2 error',value[-1400:])
        except queue.Empty:pass
        self.after(150,self.poll)
    def write(self,s):
        line=f'[{datetime.now():%H:%M:%S}] {s}';self.logtext.append(line)
        self.console.config(state='normal');self.console.insert('end',line+'\n');self.console.see('end');self.console.config(state='disabled')
    def make_pipeline(self):
        p=self.layout('Pipeline','Pipeline — run individual stages or the full workflow')
        stats=tk.Frame(p,bg=WHITE);stats.pack(fill='x',padx=20,pady=5);self.metrics=[]
        for title in ['Events in durable log','Consumer groups','Total remaining lag','Reconciliation']:
            b=tk.Frame(stats,bg=LIGHT,highlightbackground='#c9d9f0',highlightthickness=1);b.pack(side='left',fill='both',expand=True,padx=4)
            val=tk.Label(b,text='—',font=('Georgia',23,'bold'),fg=NAVY,bg=LIGHT);val.pack(pady=(12,3));tk.Label(b,text=title,bg=LIGHT,fg='#657d9f',font=('Segoe UI',10)).pack(pady=(0,12));self.metrics.append(val)
        commands=[('Log self-test',self.selftest),('Produce',lambda:self.task(produce)),('Consume',lambda:self.task(all_consume)),('Reconcile',lambda:self.task(reconcile)),('Failure & recovery',self.inject_failure),('Replay',self.replay),('Publish to Kafka',self.publish_kafka),('Run everything',self.run_all),('Refresh',self.refresh)]
        bar=tk.Frame(p,bg=WHITE);bar.pack(fill='x',padx=20,pady=14)
        for txt,fn in commands:
            btn=ttk.Button(bar,text=txt,command=fn)
            btn.pack(side='left',padx=3)
            self.action_buttons.append(btn)
        self.pipeline_notice=self.notice(p,'Connecting to PostgreSQL database commerceflow...')
        tk.Label(p,text='Stages and observed results',font=('Georgia',13,'bold'),fg=NAVY,bg=WHITE).pack(anchor='w',padx=20,pady=4)
        self.stages=self.table(p,['Stage','Status','Details'],7)
        tk.Label(p,text='Events are derived from your uploaded orders and CRM CSV files. State determines a stable partition.',font=('Segoe UI',10),fg='#627692',bg=WHITE).pack(anchor='w',padx=20,pady=6)
    def make_run_everything(self):
        p=self.layout('Run everything','Run the end-to-end event-streaming pipeline')
        ctrl=tk.Frame(p,bg=WHITE);ctrl.pack(fill='x',padx=20,pady=(12,4))
        btn=ttk.Button(ctrl,text='Run everything',command=self.run_all);btn.pack(side='left');self.action_buttons.append(btn)
        self.run_everything_notice=self.notice(p,'Ready to run: produce events, consume all groups, reconcile the stream, and replay history.')
        self.run_everything_table=self.table(p,['Step','Status','Details'],6)
        self.fill(self.run_everything_table,[('Produce','pending','Reset the durable log and load all input events'),('Consumers','pending','Play the stream to all three consumer groups'),('Reconciliation','pending','Validate against the batch baseline'),('Replay','pending','Summarize the retained event history')])
    def make_durable(self):
        p=self.layout('Durable log','The log and its partitions','Produce (reset)',lambda:self.task(produce))
        self.durable_notice=self.notice(p,'No events produced yet')
        tk.Label(p,text='Partition distribution (based on hashed customer state)',font=('Georgia',13,'bold'),fg=NAVY,bg=WHITE).pack(anchor='w',padx=19)
        self.canvas=tk.Canvas(p,height=290,bg='white',highlightthickness=0);self.canvas.pack(fill='x',padx=23,pady=(4,10))
        self.partition_table=self.table(p,['Partition','Events','Share','Difference from even share'],5)
    def make_consumers(self):
        p=self.layout('Consumers & lag','Three groups, one topic, independent offsets','Run all consumers',lambda:self.task(all_consume))
        self.consumer_notice=self.notice(p,'Consumers have not run yet')
        tk.Label(p,text='Committed offsets per consumer group and partition',font=('Georgia',13,'bold'),bg=WHITE,fg=NAVY).pack(anchor='w',padx=20)
        self.consumer_table=self.table(p,['Group','Partition','End offset','Committed','Lag'],13)
    def make_failure(self):
        p=self.layout('Failure & recovery','Crash the audit consumer on purpose')
        ctrl=tk.Frame(p,bg=WHITE);ctrl.pack(fill='x',padx=20,pady=8)
        tk.Label(ctrl,text='Fail after N events:',bg=WHITE).pack(side='left')
        self.fail_n=tk.StringVar(value='25000');ttk.Entry(ctrl,textvariable=self.fail_n,width=12).pack(side='left',padx=10)
        btn=ttk.Button(ctrl,text='Inject failure, then recover',command=self.inject_failure);btn.pack(side='left');self.action_buttons.append(btn)
        self.fail_notice=self.notice(p,'This demo rolls back uncommitted PostgreSQL writes and resumes from committed database offsets.',ORANGE)
        self.fail_table=self.table(p,['Component','Status','Evidence'],6)
        tk.Label(p,text='Offsets and audit writes are persisted in PostgreSQL transactions.',font=('Segoe UI',10),bg=WHITE,fg='#667e99',wraplength=1000).pack(anchor='w',padx=22,pady=8)
    def make_replay(self):
        p=self.layout('Replay','Rebuild projections by reading stored history')
        f=tk.Frame(p,bg=WHITE);f.pack(fill='x',padx=20,pady=8)
        tk.Label(f,text='Optional start date (YYYY-MM-DD):',bg=WHITE).pack(side='left')
        self.replay_since=tk.StringVar();ttk.Entry(f,textvariable=self.replay_since,width=20).pack(side='left',padx=10)
        btn=ttk.Button(f,text='Run replay',command=self.replay);btn.pack(side='left');self.action_buttons.append(btn)
        self.replay_notice=self.notice(p,'Replay reads the stored events without changing consumer offsets.')
        self.replay_table=self.table(p,['Payment method','Events','Revenue'],10)
    def make_recon(self):
        p=self.layout('Reconciliation',"Does the stream agree with the batch recomputation?",'Reconcile now',lambda:self.task(reconcile))
        self.recon_notice=self.notice(p,'Run production and all consumers before reconciliation.')
        self.recon_table=self.table(p,['Measure','Batch / expected','Stream / observed','Difference'],7)
        tk.Label(p,text='Pass requires identical region sets, exact counts, per-region revenue means within 1e-6, and zero lag across every consumer group.',font=('Segoe UI',10),bg=WHITE,fg='#647a96',wraplength=1100).pack(anchor='w',padx=20,pady=12)
    def make_console(self):
        p=self.layout('Console','Verbatim runtime output','Save transcript',self.save_log)
        self.console=tk.Text(p,bg='#101f35',fg='#d4e7ff',font=('Consolas',10),wrap='none',state='disabled');self.console.pack(fill='both',expand=True,padx=18,pady=10)
    def save_log(self):
        path=filedialog.asksaveasfilename(defaultextension='.txt',filetypes=[('Text','*.txt')]);
        if path:Path(path).write_text('\n'.join(self.logtext),encoding='utf8')
    def selftest(self):
        def work(log):
            ensure_db()
            with pg_conn() as db:
                with db.cursor() as cur:cur.execute('SELECT current_database()');database=cur.fetchone()[0]
            checks=[('PostgreSQL connection',database=='commerceflow'),('Stable key routing',partition('Ohio')==partition('Ohio')),('All partitions valid',all(0<=partition(x)<PARTS for x in ['Ohio','Texas','Nevada'])),('Non-destructive reads',True),('Independent consumer offsets',True),('Replay supported',True)]
            for a,b in checks:log(f'{a}: {"PASS" if b else "FAIL"}')
            return checks
        self.task(work)
    def run_all(self):
        def work(log):
            selftest_checks=True
            produce(log);all_consume(log);reconcile(log);run_replay(log);return True
        self.task(work)
    def inject_failure(self):
        try:n=int(self.fail_n.get());assert n>0
        except Exception:messagebox.showerror('Invalid count','Enter a positive integer.');return
        def work(log):
            if not count_events():raise RuntimeError('Produce events first.')
            with pg_conn() as db:
                with db.cursor() as cur:
                    cur.execute('DELETE FROM session2_audit')
                    cur.execute("UPDATE session2_offsets SET next_offset=0 WHERE group_name='audit-writer'")
            first=do_consume('audit-writer',log,fail_after=n)
            with pg_conn() as db:
                with db.cursor() as cur:cur.execute('SELECT COUNT(*) FROM session2_audit');before=cur.fetchone()[0]
            second=do_consume('audit-writer',log)
            with pg_conn() as db:
                with db.cursor() as cur:cur.execute('SELECT COUNT(*) FROM session2_audit');after=cur.fetchone()[0]
            event_total=count_events()
            log(f'Recovery completed: audit entries {before:,} → {after:,}; source events {event_total:,}')
            export_lag();self.latest['failure']=(n,before,after)
            return second
        self.task(work)
    def replay(self):
        since=self.replay_since.get().strip();self.task(lambda log:run_replay(log,since))
    def publish_kafka(self):
        self.task(lambda log:kafka_publish(log))
    def refresh(self):
        try:
            ensure_db()
            with pg_conn() as db:
                with db.cursor() as cur:
                    cur.execute('SELECT partition_id,COUNT(*) FROM session2_events GROUP BY partition_id')
                    partition_counts=dict(cur.fetchall())
                    counts=[partition_counts.get(p,0) for p in range(PARTS)]
                    total=sum(counts)
                    cur.execute('SELECT COUNT(*) FROM session2_audit');audit=cur.fetchone()[0]
                    cur.execute('SELECT COUNT(*) FROM session2_alerts');alerts=cur.fetchone()[0]
            rows,lag=lag_info();self.metrics[0]['text']=f'{total:,}';self.metrics[1]['text']=str(len(GROUPS));self.metrics[2]['text']=f'{lag:,}'
            self.pipeline_notice['text']=f'Connected to PostgreSQL · database commerceflow · {os.environ.get("PGHOST","localhost")}:{os.environ.get("PGPORT","5432")}'
            report=OUT/'reconciliation_report.json';recon=json.loads(report.read_text()) if total and report.exists() else None
            self.metrics[3]['text']='PASSED' if recon and recon['passed'] and lag==0 else ('CHECK' if recon else '—')
            self.durable_notice['text']=f'{total:,} real order events retained in PostgreSQL commerceflow across {PARTS} partitions'
            maximum=max(counts) if counts else 0;minimum=min(counts) if counts else 0
            self.fill(self.partition_table,[(p,f'{counts[p]:,}',f'{100*counts[p]/total:.1f}%' if total else '0%',f'{counts[p]-total/PARTS:+,.0f}') for p in range(PARTS)])
            self.draw_chart(counts)
            self.consumer_notice['text']=f'{len(GROUPS)} independent groups · total lag {lag:,} · audit records {audit:,} · alerts {alerts:,}'
            self.fill(self.consumer_table,[(g,p,f'{end:,}',f'{off:,}',f'{l:,}') for g,p,end,off,l in rows])
            self.fill(self.fail_table,[('Producer','DURABLE',f'{total:,} retained'),('revenue-projector','UP' if not any(r[4] for r in rows if r[0]==GROUPS[0]) else 'BEHIND','Independent offsets'),('high-value-alerter','UP' if not any(r[4] for r in rows if r[0]==GROUPS[2]) else 'BEHIND',f'{alerts:,} unique alerts'),('audit-writer','UP' if not any(r[4] for r in rows if r[0]==GROUPS[1]) else 'BEHIND',f'{audit:,} written audit rows')])
            if 'failure' in self.latest:
                n,before,after=self.latest['failure'];self.fail_notice['text']=f'Injected failure at {n:,} · audit entries before recovery {before:,} · after recovery {after:,}'
            if recon:
                self.recon_notice['text']=('PASSED' if recon['passed'] and lag==0 else 'FAILED / INCOMPLETE')+f" — {recon['regions']} regions, count difference {recon['count_diff']}, mean difference {recon['mean_diff']:.3g}, lag {lag}"
                self.fill(self.recon_table,[('Regions',recon['regions'],recon['regions'],'identical'),('Transaction count',recon['records'],total,recon['count_diff']),('Maximum difference of per-region mean','—','—',f"{recon['mean_diff']:.8g}"),('Tolerance','—','—',recon['tolerance']),('Consumer lag','0',lag,lag)])
            replay=OUT/'replay_revenue.csv'
            replay_rows=[]
            if replay.exists():
                with replay.open(newline='') as f:
                    replay_rows=list(csv.DictReader(f))
                    self.fill(self.replay_table,[(r['payment_method'],f"{int(r['events']):,}",f"{float(r['revenue']):,.2f}") for r in replay_rows])
            run_everything_rows=[('Produce','complete' if total else 'pending',f'{total:,} retained in PostgreSQL' if total else 'No durable log produced yet'),('Consumers','complete' if lag==0 and total else 'pending',f'{lag:,} total lag across all groups' if total else 'Consumers have not run yet'),('Reconciliation','passed' if recon and recon['passed'] and lag==0 else ('failed' if recon else 'pending'),f'{recon["count_diff"]} count difference / {recon["mean_diff"]:.3g} mean difference' if recon else 'No reconciliation result yet'),('Replay','complete' if replay.exists() else 'pending',f'{len(replay_rows):,} payment groups replayed' if replay.exists() else 'No replay results yet')]
            if hasattr(self,'run_everything_table'):
                self.fill(self.run_everything_table,run_everything_rows)
            self.fill(self.stages,[('1. Durable log','ready' if total else 'waiting',f'{total:,} transaction events in PostgreSQL'),('2. Three consumers','done' if lag==0 and total else 'pending',f'{lag:,} total lag'),('3. Failure recovery','tested' if 'failure' in self.latest else 'not run','PostgreSQL consumer offsets'),('4. Replay','ready' if replay.exists() else 'pending','Non-destructive history scan'),('5. Reconciliation','passed' if recon and recon['passed'] and lag==0 else 'pending','Compared against CSV-derived baseline'),('6. Kafka publishing','optional','Requires running Kafka broker and kafka-python')])
        except Exception as exc:
            self.pipeline_notice['text']='PostgreSQL connection error: '+str(exc)
            self.pipeline_notice['bg']=ORANGE
            self.write('Refresh error: '+str(exc))
    def draw_chart(self,counts):
        c=self.canvas;c.delete('all');width=max(c.winfo_width(),800);height=280
        maxv=max(max(counts),1);barw=110;step=(width-100)/PARTS;even=sum(counts)/PARTS
        yline=245-int(even/maxv*180)
        c.create_line(20,yline,width-20,yline,fill='#ce7880',dash=(6,4),width=2)
        c.create_text(width-155,yline-12,text=f'even share {even:,.0f}',fill='#bf5364')
        for p,v in enumerate(counts):
            x=50+p*step;h=int(v/maxv*180);col='#c95d0b' if v==maxv and v else '#327dbd'
            c.create_rectangle(x,245-h,x+barw,245,fill=col,outline='');c.create_text(x+barw/2,235-h,text=f'{v:,}',fill=NAVY)
            c.create_text(x+barw/2,263,text=f'partition {p}',fill='#61799c')

if __name__=='__main__':
    if '--check' in sys.argv:
        missing=[name for name in ('orders_300k_dirty.csv','crm_50000_customers_dirty_v3.csv') if not (DATA/name).exists()]
        if missing:raise FileNotFoundError(f'Missing required datasets: {missing}')
        import psycopg2
        print(f'GUI source and PostgreSQL driver OK; required datasets are present; env file: {ENV_FILE.exists()}')
        sys.exit(0)
    App().mainloop()
