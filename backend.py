"""CommerceFlow Session 2 backend: Kafka event log and PostgreSQL read models.
No fabricated results: all metrics are measured from the uploaded CSV data and actual consumers.
"""
import csv, json, os, time, hashlib, threading
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal, InvalidOperation

ROOT=Path(__file__).resolve().parent
DATA=Path(os.environ.get('COMMERCEFLOW_DATA_DIR', ROOT/'Datasets'))
RESULTS=ROOT/'results'; RESULTS.mkdir(exist_ok=True)
TOPIC=os.getenv('KAFKA_TOPIC','commerceflow.transactions.v2')
BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP','localhost:9092')
PG_DSN=os.getenv('POSTGRES_DSN','dbname=commerceflow user=commerceflow password=commerceflow host=localhost port=5432')
GROUPS=['revenue-projector','audit-writer','high-value-alerter']

def connect_pg():
    import psycopg2
    conn=psycopg2.connect(PG_DSN);conn.autocommit=False;return conn

def kafka_admin():
    from kafka.admin import KafkaAdminClient
    return KafkaAdminClient(bootstrap_servers=BOOTSTRAP,client_id='commerceflow-admin',request_timeout_ms=10000)

def ensure_infra():
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin=kafka_admin()
    try:
        try:admin.create_topics([NewTopic(name=TOPIC,num_partitions=4,replication_factor=1)],validate_only=False)
        except TopicAlreadyExistsError:pass
        desc=admin.describe_topics([TOPIC])[0]
        if len(desc['partitions']) != 4:raise ValueError(f'Topic {TOPIC} must have 4 partitions (found {len(desc["partitions"])}).')
    finally:admin.close()
    with connect_pg() as con:
        with con.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS events (event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, region TEXT NOT NULL, amount NUMERIC(20,2) NOT NULL, event_timestamp TEXT NOT NULL, partition_id INT, kafka_offset BIGINT)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS checkpoints (group_name TEXT NOT NULL, partition_id INT NOT NULL, next_offset BIGINT NOT NULL, PRIMARY KEY(group_name,partition_id))''')
            cur.execute('''CREATE TABLE IF NOT EXISTS region_revenue (region TEXT PRIMARY KEY, txn_count BIGINT NOT NULL DEFAULT 0, revenue_total NUMERIC(24,2) NOT NULL DEFAULT 0)''')
            cur.execute('''CREATE TABLE IF NOT EXISTS audit_entries (id BIGSERIAL PRIMARY KEY, event_id TEXT NOT NULL, region TEXT NOT NULL, amount NUMERIC(20,2) NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW())''')
            cur.execute('''CREATE TABLE IF NOT EXISTS alerts (event_id TEXT PRIMARY KEY, region TEXT NOT NULL, amount NUMERIC(20,2) NOT NULL)''')

def read_events(limit=60000):
    """Deduplicate CRM customer IDs and join to successful, parseable order rows."""
    customer_region={}
    with (DATA/'crm_50000_customers_dirty_v3.csv').open(newline='',encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            customer_region.setdefault(r['customer_id'].strip(), (r.get('state') or 'Unknown').strip() or 'Unknown')
    events=[];invalid=0
    with (DATA/'orders_300k_dirty.csv').open(newline='',encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            if (r.get('status') or '').strip().lower() not in ('success','completed','paid'):continue
            try:
                amt=Decimal((r.get('order_amount') or '').strip())
                if not amt.is_finite() or amt<0:raise InvalidOperation()
                amt=amt.quantize(Decimal('.01'))
            except (InvalidOperation,ValueError):invalid+=1;continue
            oid=r['order_id'].strip(); region=customer_region.get(r['customer_id'].strip(),'Unknown')
            events.append({'event_id':oid,'order_id':oid,'region':region,'amount':str(amt),'event_timestamp':r.get('order_date','')})
            if limit and len(events)>=limit:break
    if not events:raise ValueError('No valid successful orders found. Check the CSV datasets.')
    return events,invalid

def emit(log, msg):
    if log:log(msg)

def produce(log=None,limit=60000,reset=False):
    from kafka import KafkaProducer
    ensure_infra()
    if reset:
        # Reset all database materializations only. Kafka topic retention is not erased.
        # Use a fresh Kafka topic for a clean run or rely on the event IDs to avoid re-insertion.
        with connect_pg() as con:
            with con.cursor() as c:
                c.execute('TRUNCATE TABLE events, checkpoints, region_revenue, audit_entries, alerts RESTART IDENTITY')
    events,bad=read_events(limit)
    p=KafkaProducer(bootstrap_servers=BOOTSTRAP, key_serializer=lambda k:k.encode(),value_serializer=lambda v:json.dumps(v).encode(),acks='all',retries=4)
    t=time.perf_counter();partition_counts=Counter();new=0
    with connect_pg() as con:
        with con.cursor() as c:
            c.execute('SELECT event_id FROM events');known={r[0] for r in c.fetchall()}
    # Prevent inserting duplicate Kafka records on rerun. Requires clear topic for an entirely new generation.
    for event in events:
        if event['event_id'] in known:continue
        meta=p.send(TOPIC,key=event['region'],value=event).get(timeout=30)
        partition_counts[meta.partition]+=1
        with connect_pg() as con:
            with con.cursor() as c:
                c.execute('INSERT INTO events(event_id,order_id,region,amount,event_timestamp,partition_id,kafka_offset) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',(event['event_id'],event['order_id'],event['region'],event['amount'],event['event_timestamp'],meta.partition,meta.offset))
        new+=1
        if new%10000==0:emit(log,f'Appended {new:,} new events...')
    p.flush();p.close()
    secs=time.perf_counter()-t
    out={'new_events':new,'selected_orders':len(events),'invalid_amount_rows_skipped':bad,'seconds':round(secs,3),'throughput':round(new/max(secs,1e-9),2),'partitions':dict(partition_counts),'topic':TOPIC}
    (RESULTS/'producer_metrics.json').write_text(json.dumps(out,indent=2))
    emit(log,f'PRODUCER: {new:,} new events in {secs:.2f}s ({out["throughput"]:,.0f}/s); invalid amount rows skipped: {bad}')
    return out

def group_consume(name,log=None,stop_after=None,force_from_start=False):
    from kafka import KafkaConsumer, TopicPartition
    ensure_infra();assert name in GROUPS
    consumer=KafkaConsumer(bootstrap_servers=BOOTSTRAP,enable_auto_commit=False,auto_offset_reset='earliest',consumer_timeout_ms=3000,fetch_max_wait_ms=500,max_poll_records=500)
    parts=[TopicPartition(TOPIC,i) for i in range(4)];consumer.assign(parts)
    with connect_pg() as con:
        with con.cursor() as c:
            c.execute('SELECT partition_id,next_offset FROM checkpoints WHERE group_name=%s',(name,));offsets=dict(c.fetchall())
    for tp in parts:
        consumer.seek(tp,0 if force_from_start else offsets.get(tp.partition,0))
    start=time.perf_counter();processed=0;duplicates=0;batch_n=0
    try:
        # A batch is a transaction, so read-model writes + the checkpoint are atomic.
        while True:
            polled=consumer.poll(timeout_ms=800,max_records=500)
            if not polled:break
            for tp,messages in polled.items():
                for m in messages:
                    event=m.value
                    if isinstance(event,bytes):event=json.loads(event)
                    with connect_pg() as con:
                        with con.cursor() as c:
                            if name=='revenue-projector':
                                # Checkpoint guards duplicate application on restart.
                                if m.offset>=offsets.get(tp.partition,0) or force_from_start:
                                    c.execute('''INSERT INTO region_revenue(region,txn_count,revenue_total) VALUES (%s,1,%s) ON CONFLICT(region) DO UPDATE SET txn_count=region_revenue.txn_count+1,revenue_total=region_revenue.revenue_total+EXCLUDED.revenue_total''',(event['region'],event['amount']))
                                else:duplicates+=1
                            elif name=='audit-writer':
                                c.execute('INSERT INTO audit_entries(event_id,region,amount) VALUES (%s,%s,%s)',(event['event_id'],event['region'],event['amount']))
                            elif name=='high-value-alerter' and Decimal(event['amount'])>Decimal('900'):
                                c.execute('INSERT INTO alerts(event_id,region,amount) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING',(event['event_id'],event['region'],event['amount']))
                            if not force_from_start:
                                c.execute('''INSERT INTO checkpoints(group_name,partition_id,next_offset) VALUES (%s,%s,%s) ON CONFLICT(group_name,partition_id) DO UPDATE SET next_offset=GREATEST(checkpoints.next_offset,EXCLUDED.next_offset)''',(name,tp.partition,m.offset+1))
                    offsets[tp.partition]=m.offset+1;processed+=1
                    if stop_after and processed>=stop_after:break
                if stop_after and processed>=stop_after:break
            if stop_after and processed>=stop_after:break
    finally:consumer.close()
    secs=time.perf_counter()-start
    lag=sum(x['lag'] for x in offsets_report() if x['group']==name)
    result={'group':name,'processed':processed,'seconds':round(secs,3),'throughput':round(processed/max(secs,1e-9),2),'lag':lag,'duplicates_skipped':duplicates}
    emit(log,f'{name}: processed {processed:,} in {secs:.2f}s | {result["throughput"]:,.0f}/s | lag {lag:,}')
    return result

def consume_all(log=None):
    results=[group_consume(n,log) for n in GROUPS]
    with (RESULTS/'consumer_lag.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['group','partition','end_offset','committed','lag']);w.writeheader();w.writerows(offsets_report())
    (RESULTS/'consumer_metrics.json').write_text(json.dumps(results,indent=2))
    return results

def offsets_report():
    from kafka import KafkaConsumer,TopicPartition
    consumer=KafkaConsumer(bootstrap_servers=BOOTSTRAP,consumer_timeout_ms=2000)
    tps=[TopicPartition(TOPIC,i) for i in range(4)];ends=consumer.end_offsets(tps);consumer.close()
    with connect_pg() as con:
        with con.cursor() as c:c.execute('SELECT group_name,partition_id,next_offset FROM checkpoints');cps={(g,p):v for g,p,v in c.fetchall()}
    return [{'group':g,'partition':i,'end_offset':ends[tp],'committed':cps.get((g,i),0),'lag':max(0,ends[tp]-cps.get((g,i),0))} for g in GROUPS for i,tp in enumerate(tps)]

def partition_report():
    with connect_pg() as con:
        with con.cursor() as c:c.execute('SELECT partition_id, COUNT(*) FROM events GROUP BY partition_id ORDER BY partition_id');counts=dict(c.fetchall())
    vals=[counts.get(i,0) for i in range(4)];return vals

def reconciliation(log=None):
    with connect_pg() as con:
        with con.cursor() as c:
            c.execute('SELECT region,COUNT(*),SUM(amount) FROM events GROUP BY region');baseline={r:(n,Decimal(total)) for r,n,total in c.fetchall()}
            c.execute('SELECT region,txn_count,revenue_total FROM region_revenue');stream={r:(n,Decimal(total)) for r,n,total in c.fetchall()}
    regions=set(baseline)|set(stream)
    maxcount=max((abs(baseline.get(r,(0,0))[0]-stream.get(r,(0,0))[0]) for r in regions),default=0)
    maxmoney=max((abs(baseline.get(r,(0,Decimal(0)))[1]-stream.get(r,(0,Decimal(0)))[1]) for r in regions),default=Decimal(0))
    lags=offsets_report();lag=sum(x['lag'] for x in lags)
    passed=(set(baseline)==set(stream) and maxcount==0 and maxmoney<=Decimal('0.01') and lag==0)
    result={'passed':passed,'baseline_regions':len(baseline),'stream_regions':len(stream),'baseline_count':sum(x[0] for x in baseline.values()),'stream_count':sum(x[0] for x in stream.values()),'baseline_revenue':str(sum((x[1] for x in baseline.values()),Decimal(0))),'stream_revenue':str(sum((x[1] for x in stream.values()),Decimal(0))),'maximum_count_difference':maxcount,'maximum_revenue_difference':str(maxmoney),'total_lag':lag,'tolerance':'0.01'}
    (RESULTS/'reconciliation_report.json').write_text(json.dumps(result,indent=2))
    emit(log,('PASSED' if passed else 'FAILED')+f' — {len(baseline)} baseline regions, Δcount {maxcount}, Δrevenue {maxmoney}, lag {lag}')
    return result

def failure_recovery(log=None,fail_after=25000):
    # Simulates an unacknowledged window by reverting audit checkpoint only.
    # Downstream peers are untouched; duplicates in audit expose at-least-once processing.
    with connect_pg() as con:
        with con.cursor() as c:
            c.execute("SELECT COALESCE(SUM(next_offset),0) FROM checkpoints WHERE group_name='audit-writer'");before=int(c.fetchone()[0])
            if before==0:raise RuntimeError('Run consumers first to populate audit checkpoints.')
            c.execute('SELECT partition_id,next_offset FROM checkpoints WHERE group_name=%s ORDER BY partition_id',('audit-writer',));positions=c.fetchall()
            # Rewind up to 5,000 messages spread across partitions.
            remaining=min(5000,before)
            for p,v in positions:
                delta=min(remaining,v);remaining-=delta
                c.execute('UPDATE checkpoints SET next_offset=%s WHERE group_name=%s AND partition_id=%s',(v-delta,'audit-writer',p))
                if not remaining:break
    emit(log,f'Simulated audit-writer crash: rolled back {min(5000,before):,} committed offsets; producer/other groups unaffected')
    outcome=group_consume('audit-writer',log)
    with connect_pg() as con:
        with con.cursor() as c:c.execute('SELECT COUNT(*),COUNT(DISTINCT event_id) FROM audit_entries');entries,unique=c.fetchone()
    out={'simulated_redelivery':min(5000,before),'audit_entries':entries,'unique_events':unique,'duplicate_entries':entries-unique,'final_lag':outcome['lag']}
    (RESULTS/'failure_recovery.json').write_text(json.dumps(out,indent=2))
    return out

def replay(log=None,from_date='2025-01-01'):
    # A separate independent consumer reads from beginning; no changes to production checkpoints.
    from kafka import KafkaConsumer, TopicPartition
    ensure_infra();c=KafkaConsumer(bootstrap_servers=BOOTSTRAP,auto_offset_reset='earliest',consumer_timeout_ms=3000)
    tps=[TopicPartition(TOPIC,i) for i in range(4)];c.assign(tps)
    for tp in tps:c.seek_to_beginning(tp)
    sums=defaultdict(lambda:[0,Decimal(0)]);all_count=0;matched=0
    try:
        while True:
            batches=c.poll(timeout_ms=800,max_records=1500)
            if not batches:break
            for ms in batches.values():
                for m in ms:
                    e=json.loads(m.value);all_count+=1
                    if e['event_timestamp'][:10]>=from_date:
                        matched+=1;key=e['region'];sums[key][0]+=1;sums[key][1]+=Decimal(e['amount'])
    finally:c.close()
    rows=[{'region':r,'events':v[0],'revenue':str(v[1])} for r,v in sorted(sums.items())]
    with (RESULTS/'replay_region_revenue.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['region','events','revenue']);w.writeheader();w.writerows(rows)
    result={'scanned':all_count,'matched':matched,'from_date':from_date,'regions':len(rows),'revenue':str(sum((x[1] for x in sums.values()),Decimal(0)))}
    emit(log,f'REPLAY: scanned {all_count:,}, matched {matched:,} from {from_date}, revenue {result["revenue"]}')
    return result

def snapshot():
    with connect_pg() as con:
        with con.cursor() as c:
            c.execute('SELECT COUNT(*) FROM events');events=c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM audit_entries');audit=c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM alerts');alerts=c.fetchone()[0]
    try:lags=offsets_report();lag=sum(r['lag'] for r in lags)
    except Exception:lag=None
    return {'events':events,'audit':audit,'alerts':alerts,'lag':lag}
