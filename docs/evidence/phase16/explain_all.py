import json, base64, urllib.request, time, re, sys
conf=json.load(open('apiS.json'))
auth=base64.b64encode(f"{conf['key']}:{conf['secret']}".encode()).decode()
hdr={"Content-Type":"application/json","Authorization":f"Basic {auth}"}
base=f"https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/{conf['org']}/environments/{conf['env']}/statements"
P={"sql.current-catalog":"flink-fable5","sql.current-database":"flink-fable5-kafka"}

src=open('/Users/jimzucker/code/GitHub/flink-fable5/confluent/sql/optimized/statement_set.sql').read()
body=src.split('BEGIN',1)[1].rsplit('END;',1)[0]
parts=[p.strip() for p in re.split(r';\s*(?=INSERT INTO)', body) if p.strip()]
parts=[p.rstrip(';').strip()+';' for p in parts]
print(f"parsed {len(parts)} INSERT statements\n")

def explain(name, sql):
    st={"name":name,"organization_id":conf['org'],"environment_id":conf['env'],
        "spec":{"statement":"EXPLAIN "+sql,"compute_pool_id":conf['pool'],
                "principal":conf['sa'],"properties":P}}
    try:
        urllib.request.urlopen(urllib.request.Request(base,data=json.dumps(st).encode(),method="POST",headers=hdr),timeout=90)
    except urllib.error.HTTPError as e:
        print(f"  {name}: SUBMIT ERR {e.code} {e.read().decode()[:200]}"); return
    for _ in range(40):
        time.sleep(4)
        try:
            with urllib.request.urlopen(urllib.request.Request(f"{base}/{name}",headers=hdr),timeout=30) as r:
                d=json.loads(r.read())
            ph=d['status']['phase']
            if ph in ("COMPLETED","FAILED"):
                if ph=="FAILED": print(f"  {name}: FAILED {d['status'].get('detail','')[:200]}"); return
                break
        except Exception: pass
    try:
        with urllib.request.urlopen(urllib.request.Request(f"{base}/{name}/results",headers=hdr),timeout=30) as r:
            res=json.loads(r.read())
        txt=json.dumps(res)
        for m in re.finditer(r'([A-Z][A-Z_]{8,})', txt):
            pass
        rows=res.get('results',{}).get('data',[])
        out="\n".join(str(x.get('row',x)) for x in rows)
        return out
    except Exception as e:
        print(f"  {name}: results err {e}"); return

targets=["position-by-account-ticker","position-by-ticker","mv-by-account-ticker","mv-by-ticker"]
for i,p in enumerate(parts):
    nm=f"exp3-{i}-{targets[i] if i<len(targets) else i}"[:60]
    try: urllib.request.urlopen(urllib.request.Request(f"{base}/{nm}",method="DELETE",headers=hdr),timeout=30)
    except Exception: pass
    out=explain(nm,p)
    print(f"===== {targets[i] if i<len(targets) else i} =====")
    if out:
        # surface only advisory/warning lines
        keep=[l for l in out.split('\\n') if re.search(r'advice|warn|WARNING|ADVICE|upsert|UPSERT|TTL|state|STATE',l,re.I)]
        print("\n".join(keep[:60]) if keep else out[:1500])
    print()
