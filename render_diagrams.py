"""
render_diagrams.py — recreate the three CommerceFlow Session 1 diagrams from code.

Outputs:
- docs/ecommerce_uml.png
- docs/entity-model-session1.png
- architecture/architecture-session1.png

Graphviz's ``dot`` executable must be installed and available on PATH.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import config as cfg


BLUE = "#2E74B5"
LIGHT_BLUE = "#D9EAF7"
AMBER = "#C55A11"
LIGHT_AMBER = "#FFF2CC"
GREEN = "#548235"
LIGHT_GREEN = "#E2EFDA"
GREY = "#767171"
NAVY = "#1F3864"


def evidence() -> dict:
    """Load measured values produced by the pipeline, with safe fallbacks."""
    data = {
        "clickstream_rows": 500_000,
        "customer_raw_rows": 50_000,
        "customer_rows": 48_200,
        "orders_rows": 300_000,
        "product_rows": 500,
        "support_rows": 30_000,
        "states": 50,
        "min_per_state": 5_547,
        "median_per_state": 5_981,
        "max_per_state": 6_348,
        "skew": 1.14,
        "crm_duplicate_rows": 1_800,
    }

    if cfg.OUT_PROFILE.exists():
        prof = json.loads(cfg.OUT_PROFILE.read_text(encoding="utf-8"))
        p = prof.get("profiles", {})
        data["clickstream_rows"] = p.get("clickstream", {}).get(
            "rows", data["clickstream_rows"]
        )
        data["customer_raw_rows"] = p.get("customers", {}).get(
            "rows", data["customer_raw_rows"]
        )
        data["orders_rows"] = p.get("orders", {}).get(
            "rows", data["orders_rows"]
        )
        data["product_rows"] = p.get("products", {}).get(
            "rows", data["product_rows"]
        )
        data["support_rows"] = p.get("support", {}).get(
            "rows", data["support_rows"]
        )
        integ = prof.get("integrity", {})
        data["crm_duplicate_rows"] = integ.get(
            "crm_duplicate_rows_to_remove",
            data["crm_duplicate_rows"],
        )
        data["customer_rows"] = (
            data["customer_raw_rows"] - data["crm_duplicate_rows"]
        )

    if cfg.OUT_PARTITION_STRATEGY.exists():
        part = json.loads(
            cfg.OUT_PARTITION_STRATEGY.read_text(encoding="utf-8")
        )
        pred = part.get("prediction", {})
        data["states"] = pred.get("distinct", data["states"])
        data["min_per_state"] = pred.get("min", data["min_per_state"])
        data["median_per_state"] = pred.get(
            "median", data["median_per_state"]
        )
        data["max_per_state"] = pred.get("max", data["max_per_state"])
        data["skew"] = pred.get("skew_ratio", data["skew"])

    return data


def entity_dot(d: dict) -> str:
    return f"""
digraph EntityModel {{
  rankdir=TB;
  bgcolor="white";
  splines=polyline;
  nodesep=0.95;
  ranksep=1.05;
  fontname="Helvetica";
  labelloc="t";
  fontsize=18;
  label=<<b>CommerceFlow — Session 1 Entity Model</b><br/>
         <font point-size="11">B2C e-commerce revenue intelligence dataset</font><br/>>;

  node [shape=plaintext fontname="Helvetica"];
  edge [color="{BLUE}" fontname="Helvetica" fontsize=10 penwidth=1.6 arrowhead=none];

  Product [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT_BLUE}"><b>Product</b><br/><font point-size="9">Entity · {d["product_rows"]:,} rows</font></td></tr>
      <tr><td align="left"><b>product_id : String «PK»</b></td></tr>
      <tr><td align="left">product_name : String</td></tr>
      <tr><td align="left">category : String</td></tr>
      <tr><td align="left">price : Decimal</td></tr>
    </table>
  >];

  Customer [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT_BLUE}"><b>Customer</b><br/><font point-size="9">Entity · {d["customer_raw_rows"]:,} raw rows · {d["customer_rows"]:,} unique IDs</font></td></tr>
      <tr><td align="left"><b>customer_id : UUID «natural key»</b></td></tr>
      <tr><td align="left">signup_date : Date</td></tr>
      <tr><td align="left" bgcolor="#FFF2CC">state : String <b>«partitionKey»</b></td></tr>
      <tr><td align="left">source : String</td></tr>
    </table>
  >];

  Order [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT_BLUE}"><b>Order</b><br/><font point-size="9">Event · {d["orders_rows"]:,} rows</font></td></tr>
      <tr><td align="left"><b>order_id : UUID «PK»</b></td></tr>
      <tr><td align="left">customer_id : UUID «FK»</td></tr>
      <tr><td align="left">product_id : String «FK»</td></tr>
      <tr><td align="left">order_amount : Decimal</td></tr>
      <tr><td align="left">order_date : Date «eventTime»</td></tr>
      <tr><td align="left">status : String</td></tr>
    </table>
  >];

  Clickstream [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT_BLUE}"><b>ClickstreamEvent</b><br/><font point-size="9">Event · {d["clickstream_rows"]:,} rows</font></td></tr>
      <tr><td align="left"><b>event_id : UUID «PK»</b></td></tr>
      <tr><td align="left">customer_id : UUID «optional FK»</td></tr>
      <tr><td align="left">session_id : UUID</td></tr>
      <tr><td align="left">event_type : String</td></tr>
      <tr><td align="left">timestamp : Timestamp</td></tr>
    </table>
  >];

  Support [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT_BLUE}"><b>SupportTicket</b><br/><font point-size="9">Event · {d["support_rows"]:,} rows</font></td></tr>
      <tr><td align="left"><b>ticket_id : UUID «PK»</b></td></tr>
      <tr><td align="left">customer_id : UUID «FK»</td></tr>
      <tr><td align="left">issue_type : String</td></tr>
      <tr><td align="left">resolution_time_hours : Double</td></tr>
      <tr><td align="left">sentiment : String</td></tr>
    </table>
  >];

  Product -> Order [taillabel="1" headlabel="0..*" label="appears in" labeldistance=2.1];
  Customer -> Order [taillabel="1" headlabel="0..*" label="places" color="{AMBER}" penwidth=2.4 labeldistance=2.1];
  Customer -> Clickstream [taillabel="0..1" headlabel="0..*" label="generates (anonymous allowed)" style=dashed labeldistance=2.1];
  Customer -> Support [taillabel="1" headlabel="0..*" label="opens" labeldistance=2.1];

  note [shape=note style=filled fillcolor="#FFF9E6" color="{AMBER}" fontsize=10 label=<
    <b>Partition key: state</b><br align="left"/>
    {d["states"]} distinct values<br align="left"/>
    {d["min_per_state"]:,} / {d["median_per_state"]:,} / {d["max_per_state"]:,} orders per state<br align="left"/>
    skew ratio {float(d["skew"]):.2f} : 1<br align="left"/><br align="left"/>
    <b>Monetization:</b> normalized successful,<br align="left"/>
    positive-value orders produce revenue_amount.<br align="left"/>
    CRM contains {d["crm_duplicate_rows"]:,} duplicate rows and is canonicalized first.
  >];

  note -> Customer [style=invis];
  {{ rank=same; note; Customer; Product; }}
}}
"""


def uml_dot(d: dict) -> str:
    return f"""
digraph UML {{
  rankdir=TB;
  bgcolor="white";
  splines=ortho;
  nodesep=0.75;
  ranksep=0.9;
  fontname="Helvetica";
  labelloc="t";
  fontsize=17;
  label=<<b>UML Class Diagram — E-Commerce Multi-File Dataset</b>>;

  node [shape=plaintext fontname="Helvetica"];
  edge [color="#666666" fontname="Helvetica" fontsize=10 arrowhead=none];

  Product [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="#E2EFDA"><b>Product</b></td></tr>
      <tr><td align="left"><b>PK</b> product_id</td></tr>
      <tr><td align="left">product_name</td></tr>
      <tr><td align="left">category</td></tr>
      <tr><td align="left">price</td></tr>
    </table>
  >];

  Customer [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="{LIGHT_BLUE}"><b>Customer</b></td></tr>
      <tr><td align="left"><b>PK?</b> customer_id (dirty duplicates)</td></tr>
      <tr><td align="left">first_name, last_name</td></tr>
      <tr><td align="left">email, phone_number</td></tr>
      <tr><td align="left">gender, dob, signup_date</td></tr>
      <tr><td align="left">address, city, state, country</td></tr>
      <tr><td align="left">device_id(s), source</td></tr>
    </table>
  >];

  Order [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="#FFF2CC"><b>Order</b></td></tr>
      <tr><td align="left"><b>PK</b> order_id</td></tr>
      <tr><td align="left"><b>FK</b> customer_id</td></tr>
      <tr><td align="left"><b>FK</b> product_id</td></tr>
      <tr><td align="left">order_amount</td></tr>
      <tr><td align="left">order_date</td></tr>
      <tr><td align="left">payment_method</td></tr>
      <tr><td align="left">status, quantity</td></tr>
    </table>
  >];

  Clickstream [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="#F4DDF4"><b>ClickstreamEvent</b></td></tr>
      <tr><td align="left"><b>PK</b> event_id</td></tr>
      <tr><td align="left">session_id</td></tr>
      <tr><td align="left"><b>FK?</b> customer_id (nullable)</td></tr>
      <tr><td align="left">event_type, page_url</td></tr>
      <tr><td align="left">device_id, timestamp, ingest_run_id</td></tr>
    </table>
  >];

  Support [label=<
    <table border="0" cellborder="1" cellspacing="0" cellpadding="5">
      <tr><td bgcolor="#F4CCCC"><b>SupportTicket</b></td></tr>
      <tr><td align="left"><b>PK</b> ticket_id</td></tr>
      <tr><td align="left"><b>FK</b> customer_id</td></tr>
      <tr><td align="left">issue_type</td></tr>
      <tr><td align="left">ticket_created, ticket_resolved</td></tr>
      <tr><td align="left">resolution_time_hours</td></tr>
      <tr><td align="left">sentiment, support_agent</td></tr>
    </table>
  >];

  Product -> Order [label="1        0..*"];
  Customer -> Order [label="1        0..*"];
  Customer -> Clickstream [label="0..1        0..*"];
  Customer -> Support [label="1        0..*"];

  note [shape=note style=filled fillcolor="#F2F2F2" label="Relationship checks from the CSVs:\\l• all order customer_ids resolve to CRM\\l• all support customer_ids resolve to CRM\\l• all non-null clickstream customer_ids resolve to CRM\\l• all order product_ids resolve to Product\\l• CRM customer_id has duplicate raw rows\\l"];
  Order -> note [style=dashed];
}}
"""


def architecture_dot(d: dict) -> str:
    return f"""
digraph Architecture {{
  rankdir=LR;
  bgcolor="white";
  compound=true;
  nodesep=0.42;
  ranksep=0.9;
  fontname="Helvetica";
  labelloc="t";
  fontsize=17;
  label=<<b>CommerceFlow — Session 1 In-Memory Parallel Compute Architecture</b><br/>
         <font point-size="11">CRM + clickstream + product + order + support analytics</font><br/>>;

  node [shape=box style="rounded,filled" fontname="Helvetica" fontsize=10 penwidth=1.3 margin="0.14,0.09"];
  edge [color="{BLUE}" penwidth=1.4 fontname="Helvetica" fontsize=9];

  subgraph cluster_source {{
    label=<<b>Source CSVs</b>>;
    color="{GREY}";
    style=dashed;

    click [label="clickstream_500k_events.csv\\n{d["clickstream_rows"]:,} events" fillcolor="{LIGHT_BLUE}"];
    support [label="support_tickets_30000_dirty.csv\\n{d["support_rows"]:,} tickets" fillcolor="{LIGHT_BLUE}"];
    orders [label="orders_300k_dirty.csv\\n{d["orders_rows"]:,} order events" fillcolor="{LIGHT_BLUE}"];
    crm [label="crm_50000_customers_dirty_v3.csv\\n{d["customer_raw_rows"]:,} CRM rows" fillcolor="{LIGHT_BLUE}"];
    product [label="product_catalog_dirty_30pct.csv\\n{d["product_rows"]:,} products" fillcolor="{LIGHT_BLUE}"];
  }}

  subgraph cluster_integrate {{
    label=<<b>Profiling + Integration</b>>;
    color="{GREY}";
    style=dashed;

    reduce [label="customer-level feature reduction\\nclickstream + support" fillcolor="{LIGHT_AMBER}" color="{AMBER}"];
    profile [label="profile_files.py\\neligibility + integrity + quality" fillcolor="{LIGHT_AMBER}" color="{AMBER}"];
    join [label="load_and_join.py\\norder-grain working dataset\\n{d["orders_rows"]:,} → {d["orders_rows"]:,}" fillcolor="{LIGHT_AMBER}" color="{AMBER}"];
  }}

  subgraph cluster_compute {{
    label=<<b>Parallel Compute</b>>;
    color="{GREY}";
    style=dashed;

    repart [label="repartition({cfg.CHOSEN_PARTITIONS}, 'state')\\nbounded parallelism" fillcolor="{LIGHT_GREEN}" color="{GREEN}"];
    agg [label="parallel_compute.py\\ngroupBy(state)\\norders · successful orders · revenue · AOV" fillcolor="{LIGHT_GREEN}" color="{GREEN}"];
    base [label="sequential_baseline.py\\npandas reference" fillcolor="#FCE4D6" color="{AMBER}"];
    valid [label="validate()\\ncounts exact · floats < {cfg.TOLERANCE:.0e}" fillcolor="#FCE4D6" color="{AMBER}"];
  }}

  subgraph cluster_results {{
    label=<<b>Results</b>>;
    color="{GREY}";
    style=dashed;

    parts [label="partition_sizes.csv" fillcolor="white"];
    parquet [label="state_revenue.parquet" fillcolor="{LIGHT_BLUE}" color="{NAVY}" penwidth=2.1];
    bench [label="session1_benchmark.csv" fillcolor="white"];
  }}

  click -> reduce;
  support -> reduce;
  click -> profile [style=dotted];
  support -> profile [style=dotted];
  orders -> profile;
  crm -> profile;
  product -> profile;

  reduce -> join;
  profile -> join [label="eligible"];
  orders -> join [style=dotted];
  crm -> join [style=dotted];
  product -> join [style=dotted];

  join -> repart;
  join -> base [label="same logical data"];
  repart -> agg;
  agg -> valid;
  base -> valid [label="reference"];
  valid -> parquet [label="passed" color="{GREEN}" penwidth=2.0];

  repart -> parts [style=dotted];
  agg -> bench [style=dotted];
}}
"""


def session2_entity_dot() -> str:
    return """
digraph Session2EntityModel {
  rankdir=TB;
  bgcolor="white";
  splines=polyline;
  nodesep=0.9;
  ranksep=1.1;
  fontname="Helvetica";
  fontsize=18;
  label=<<b>CommerceFlow — Session 2 Entity Model</b><br/><font point-size="11">Durable event log with partitioned consumers</font><br/>>;

  node [shape=plaintext fontname="Helvetica"];
  edge [color="#2E74B5" fontname="Helvetica" fontsize=10 penwidth=1.6 arrowhead=none];

  EventLog [label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6">
    <tr><td bgcolor="#D9EAF7"><b>session2_events</b><br/><font point-size="9">Durable log table</font></td></tr>
    <tr><td align="left"><b>partition_id : int</b></td></tr>
    <tr><td align="left"><b>event_offset : bigint</b></td></tr>
    <tr><td align="left"><b>event_id : text</b></td></tr>
    <tr><td align="left">customer_id : text</td></tr>
    <tr><td align="left">product_id : text</td></tr>
    <tr><td align="left">region : text</td></tr>
    <tr><td align="left">order_amount : text</td></tr>
    <tr><td align="left">order_date : text</td></tr>
    <tr><td align="left">payment_method : text</td></tr>
    <tr><td align="left">status : text</td></tr>
    <tr><td align="left">quantity : text</td></tr>
    <tr><td align="left">revenue : double precision</td></tr>
  </table>>];

  ConsumerOffsets [label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6">
    <tr><td bgcolor="#FFF2CC"><b>session2_offsets</b><br/><font point-size="9">Consumer checkpoints</font></td></tr>
    <tr><td align="left"><b>group_name : text</b></td></tr>
    <tr><td align="left"><b>partition_id : int</b></td></tr>
    <tr><td align="left"><b>next_offset : bigint</b></td></tr>
  </table>>];

  AuditTable [label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6">
    <tr><td bgcolor="#E2EFDA"><b>session2_audit</b><br/><font point-size="9">Derived side effects</font></td></tr>
    <tr><td align="left"><b>audit_id : bigserial</b></td></tr>
    <tr><td align="left">event_id : text</td></tr>
    <tr><td align="left">region : text</td></tr>
    <tr><td align="left">revenue : double precision</td></tr>
    <tr><td align="left">recorded_at : timestamptz</td></tr>
  </table>>];

  AlertTable [label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6">
    <tr><td bgcolor="#F4CCCC"><b>session2_alerts</b><br/><font point-size="9">High-value anomaly table</font></td></tr>
    <tr><td align="left"><b>event_id : text</b></td></tr>
    <tr><td align="left">region : text</td></tr>
    <tr><td align="left">revenue : double precision</td></tr>
  </table>>];

  Producer [label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6">
    <tr><td bgcolor="#D9EAF7"><b>Producer</b><br/><font point-size="9">CSV → durable log</font></td></tr>
    <tr><td align="left">orders_300k_dirty.csv</td></tr>
    <tr><td align="left">crm_50000_customers_dirty_v3.csv</td></tr>
  </table>>];

  ConsumerGroup [label=<<table border="0" cellborder="1" cellspacing="0" cellpadding="6">
    <tr><td bgcolor="#FFF2CC"><b>Consumer Group</b><br/><font point-size="9">revenue-projector / audit-writer / high-value-alerter</font></td></tr>
    <tr><td align="left">reads durable log</td></tr>
    <tr><td align="left">tracks committed offsets</td></tr>
    <tr><td align="left">writes side effects</td></tr>
  </table>>];

  Producer -> EventLog [label="ingests 300k order events" labeldistance=2.4];
  EventLog -> ConsumerGroup [label="streams ordered records" labeldistance=2.4];
  ConsumerGroup -> ConsumerOffsets [label="updates next_offset" labeldistance=2.4];
  ConsumerGroup -> AuditTable [label="writes audit rows" labeldistance=2.4];
  ConsumerGroup -> AlertTable [label="emits alerts when revenue > 900" labeldistance=2.4];

  note [shape=note style=filled fillcolor="#FFF9E6" color="#C55A11" fontsize=10 label=<<b>Key partitioning rule</b><br align="left"/>Customer state determines a stable partition<br align="left"/>4 logical partitions are used<br align="left"/>Replay reads the log without mutating offsets>];
  EventLog -> note [style=invis];
}
"""


def session2_architecture_dot() -> str:
    return """
digraph Session2Architecture {
  rankdir=LR;
  bgcolor="white";
  compound=true;
  nodesep=0.48;
  ranksep=0.95;
  fontname="Helvetica";
  fontsize=17;
  label=<<b>CommerceFlow — Session 2 Event Streaming Architecture</b><br/><font point-size="11">PostgreSQL durable log + partitioned consumers + reconciliation</font><br/>>;

  node [shape=box style="rounded,filled" fontname="Helvetica" fontsize=10 penwidth=1.3 margin="0.16,0.10"];
  edge [color="#2E74B5" penwidth=1.5 fontname="Helvetica" fontsize=9];

  subgraph cluster_sources {
    label=<<b>Source data</b>>;
    color="#767171";
    style=dashed;
    crm [label="crm_50000_customers_dirty_v3.csv\ncustomer state lookup" fillcolor="#D9EAF7"];
    orders [label="orders_300k_dirty.csv\nraw order events" fillcolor="#D9EAF7"];
  }

  subgraph cluster_ingest {
    label=<<b>Ingestion / durable log</b>>;
    color="#767171";
    style=dashed;
    produce [label="produce()\ntruncate + reload\nsession2_events" fillcolor="#FFF2CC" color="#C55A11"];
    log [label="PostgreSQL\nsession2_events\n4 partitions by customer state" fillcolor="#D9EAF7" color="#1F3864" penwidth=2.1];
    offsets [label="session2_offsets\nper-group committed offsets" fillcolor="#FFF2CC" color="#C55A11"];
  }

  subgraph cluster_consumers {
    label=<<b>Consumer layer</b>>;
    color="#767171";
    style=dashed;
    rev [label="revenue-projector\nrebuild regional revenue" fillcolor="#E2EFDA" color="#548235"];
    audit [label="audit-writer\nappend audit rows" fillcolor="#E2EFDA" color="#548235"];
    alert [label="high-value-alerter\nalert on revenue > 900" fillcolor="#E2EFDA" color="#548235"];
  }

  subgraph cluster_support {
    label=<<b>Operational services</b>>;
    color="#767171";
    style=dashed;
    reconcile [label="reconcile()\ncompare against CSV batch baseline" fillcolor="#F4CCCC" color="#A85454"];
    replay [label="run_replay()\nread event history without offset mutation" fillcolor="#F4CCCC" color="#A85454"];
    kafka [label="kafka_publish()\noptional Kafka topic\ntransaction.recorded" fillcolor="#F4CCCC" color="#A85454"];
  }

  subgraph cluster_results {
    label=<<b>Output artifacts</b>>;
    color="#767171";
    style=dashed;
    lag [label="consumer_lag.csv" fillcolor="white"];
    revenue [label="streamed_regional_revenue.csv" fillcolor="white"];
    recon [label="reconciliation_report.json" fillcolor="white"];
    replay_csv [label="replay_revenue.csv" fillcolor="white"];
  }

  crm -> produce;
  orders -> produce;
  produce -> log;
  log -> rev;
  log -> audit;
  log -> alert;
  rev -> offsets;
  audit -> offsets;
  alert -> offsets;

  rev -> revenue;
  audit -> lag;
  alert -> lag;
  log -> reconcile;
  reconcile -> recon;
  log -> replay;
  replay -> replay_csv;
  log -> kafka;
  kafka -> "Kafka broker\ntransaction.recorded" [style=dotted];
}
"""


def render(dot_source: str, out_png: Path) -> None:
    out_png.parent.mkdir(parents=True, exist_ok=True)
    dot_file = out_png.with_suffix(".dot")
    dot_file.write_text(dot_source, encoding="utf-8")

    dot_exe = shutil.which("dot")
    if not dot_exe:
        raise RuntimeError(
            "Graphviz 'dot' was not found on PATH. "
            f"The DOT source was saved at {dot_file}."
        )

    subprocess.run(
        [
            dot_exe,
            "-Tpng",
            "-Gdpi=150",
            str(dot_file),
            "-o",
            str(out_png),
        ],
        check=True,
    )
    print(f"Wrote {out_png}")


def main() -> int:
    d = evidence()
    render(uml_dot(d), cfg.OUT_UML_DIAGRAM)
    render(entity_dot(d), cfg.OUT_ENTITY_DIAGRAM)
    render(architecture_dot(d), cfg.OUT_ARCH_DIAGRAM)
    render(session2_entity_dot(), cfg.OUT_SESSION2_ENTITY)
    render(session2_architecture_dot(), cfg.OUT_SESSION2_ARCH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
