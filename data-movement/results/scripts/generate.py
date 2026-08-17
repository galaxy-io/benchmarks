import argparse
import html
import json
import re
import statistics
from pathlib import Path

RESULTS = Path(__file__).resolve().parent.parent
TEMPLATE = Path(__file__).resolve().parent / "template.html"

ROUTES = ["pg-pg", "pg-mysql", "mysql-pg", "mysql-mysql", "pg-iceberg", "mysql-iceberg"]
ROUTE_LABEL = {
    "pg-pg": "from Postgres 16 to Postgres 16",
    "pg-mysql": "from Postgres 16 to MySQL 8.4",
    "mysql-pg": "from MySQL 8.4 to Postgres 16",
    "mysql-mysql": "from MySQL 8.4 to MySQL 8.4",
    "pg-iceberg": "from Postgres 16 to Apache Iceberg",
    "mysql-iceberg": "from MySQL 8.4 to Apache Iceberg",
}
# display order within charts is by median wall ascending; table order matches.
BASELINE = "native"  # hatched bar

def fmt_bytes(n):
    if n >= 1e9: return f"{n/1e9:.2f} GB"
    if n >= 1e6: return f"{n/1e6:.0f} MB"
    return f"{n/1e3:.0f} KB"

def fmt_s(v, prec=3):
    return f"{v:.{prec}f}s"

def dataset_order(ds):
    if ds.startswith("tpch-sf"):
        return (2, float(ds.removeprefix("tpch-sf")))
    if ds.startswith("taxi-") and ds.endswith("mo"):
        return (1, float(ds.removeprefix("taxi-").removesuffix("mo")))
    return (0, ds)

def dataset_label(ds):
    if ds.startswith("tpch-sf"):
        return "TPC-H SF " + ds.removeprefix("tpch-sf")
    if ds.startswith("taxi-") and ds.endswith("mo"):
        return "NYC Taxi " + ds.removeprefix("taxi-").removesuffix("mo") + " mo"
    return ds

def result_paths(date, cohort):
    date_dir = RESULTS / date
    if cohort:
        paths = sorted((date_dir / cohort).glob("*/*.json"))
        if not paths:
            raise SystemExit(f"no results for cohort {cohort!r} on {date}")
        return paths, date_dir / cohort / "index.html", cohort
    cohorts = sorted(p for p in date_dir.iterdir() if p.is_dir() and list(p.glob("*/*.json"))) if date_dir.exists() else []
    if len(cohorts) > 1:
        names = ", ".join(p.name for p in cohorts)
        raise SystemExit(f"multiple cohorts for {date}: {names}; pass --cohort")
    if len(cohorts) == 1:
        return sorted(cohorts[0].glob("*/*.json")), cohorts[0] / "index.html", cohorts[0].name
    return sorted(date_dir.glob("*/*.json")), date_dir / "index.html", "legacy"

def load(paths):
    data = {}
    metadata = None
    route_endpoints = {}
    for p in paths:
        d = json.loads(p.read_text())
        if not d.get("parityPass"):
            continue
        identity = (
            d.get("cohort", "legacy"),
            d.get("topology", "local"),
            d.get("machine", ""),
            d.get("gitCommit", ""),
            d.get("gitDirty", False),
            d.get("verification", "row-count"),
        )
        if metadata is None:
            metadata = identity
        elif identity != metadata:
            raise SystemExit(f"mixed result cohorts/topologies/machines/commits: {p}")
        endpoints = (
            json.dumps(d.get("source", {}), sort_keys=True),
            json.dumps(d.get("sink", {}), sort_keys=True),
        )
        if d["route"] in route_endpoints and endpoints != route_endpoints[d["route"]]:
            raise SystemExit(f"mixed source/sink metadata for route {d['route']}: {p}")
        route_endpoints[d["route"]] = endpoints
        data.setdefault(d["dataset"], {}).setdefault(d["route"], []).append(d)
    if not data:
        raise SystemExit("no passing result JSON files found")
    for ds in data:
        for route in data[ds]:
            seen = set()
            for d in data[ds][route]:
                if d["sut"] in seen:
                    raise SystemExit(f"duplicate {d['sut']} result for {ds}/{route}")
                seen.add(d["sut"])
            rows = {d["rows"] for d in data[ds][route]}
            reps = {len(d["reps"]) for d in data[ds][route]}
            if len(rows) != 1:
                raise SystemExit(f"mixed row totals for {ds}/{route}: {sorted(rows)}")
            if len(reps) != 1:
                raise SystemExit(f"mixed repetition counts for {ds}/{route}: {sorted(reps)}")
            data[ds][route].sort(key=lambda d: d["medianWallSeconds"])
    return dict(sorted(data.items(), key=lambda kv: dataset_order(kv[0]), reverse=True))

def median_rep(d):
    return min(d["reps"], key=lambda r: abs(r["wallSeconds"] - d["medianWallSeconds"]))

def tool_roles(rep):
    return [u for u in rep.get("resources", []) if u["role"] not in ("source", "sink")]

def db_roles(rep):
    return [u for u in rep.get("resources", []) if u["role"] in ("source", "sink")]

def tool_mem(d):
    return sum(u["peakMemBytes"] for u in tool_roles(median_rep(d)))

def tool_cpu(d):
    return sum(u["cpuSeconds"] for u in tool_roles(median_rep(d)))

def sut_class(name):
    return name  # css classes are per-sut

DBZ_NOTE = "A CDC engine; a full load measures its initial snapshot, not the steady-state streaming it is built for."

def disp(name):
    if name == "debezium":
        return f'debezium&thinsp;<span class="hint" data-tip="{DBZ_NOTE}">*</span>'
    return name

def tip(d):
    cfg = " · ".join(f"{k} {v}" for k, v in sorted(d.get("config", {}).items()))
    t = f"{d['image']}. {cfg}" if cfg else d["image"]
    if d["sut"] == "debezium":
        t += " A CDC engine measured on its initial snapshot."
    return html.escape(t, quote=True)

def bar_rows(items, unit_fmt, single=False, rate=None):
    if not items:
        return '        <div class="sectionnote">No positive resource sample was captured.</div>'
    mx = max(v for _, v in items) or 1
    out = []
    for i, (d, v) in enumerate(items):
        name = disp(d["sut"])
        w = v / mx * 100
        extra = ""
        if not single:
            r = "fastest" if i == 0 else f"{v / items[0][1]:.1f}&times; slower"
            extra = f'\n          <div class="rate mono">{r}</div>'
        out.append(f'''        <div class="row">
          <div class="name">{name}</div>
          <div class="track"><div class="bar {sut_class(d["sut"])}" style="width: {w:.1f}%"></div></div>
          <div class="time mono">{unit_fmt(v)}</div>{extra}
        </div>''')
    return "\n".join(out)

# display order and names follow the data path: source db, the tool's
# containers, sink db. airbyte's stripped connector roles would otherwise
# collide with the database rows.
CTR_ORDER = {"source": 0, "airbyte-source": 1, "airbyte-destination": 2, "airbyte": 3, "sink": 9}
CTR_NAME = {"airbyte": "driver", "airbyte-source": "connector (source)", "airbyte-destination": "connector (destination)"}

def ctr_order(u):
    return CTR_ORDER.get(u["role"], 5)

def containers_table(route_runs):
    rows = []
    for gi, d in enumerate(route_runs):
        last_group = gi == len(route_runs) - 1
        rep = median_rep(d)
        res = sorted(rep.get("resources", []), key=ctr_order)
        n = len(res)
        first = True
        for u in res:
            dim = " dim" if u["role"] in ("source", "sink") else ""
            cls = ' class="lastgroup"' if last_group else ""
            lead = (f'<td rowspan="{n}"{cls}><span class="swatch {sut_class(d["sut"])}"></span>{disp(d["sut"])}</td>\n              '
                    if first else "")
            role = CTR_NAME.get(u["role"], u["role"])
            rows.append(f'''            <tr>
              {lead}<td class="mono{dim}">{role}</td>
              <td class="num mono{dim}">{u["cpuSeconds"]:.2f}s</td>
              <td class="num mono{dim}">{fmt_bytes(u["peakMemBytes"])}</td>
              <td class="num mono{dim}">{fmt_bytes(u["netRxBytes"])}</td>
              <td class="num mono{dim}">{fmt_bytes(u["netTxBytes"])}</td>
            </tr>''')
            first = False
    return "\n".join(rows)

def runs_table(route_runs):
    rows = []
    for d in route_runs:
        walls = sorted(r["wallSeconds"] for r in d["reps"])
        setup = statistics.median(r["setupSeconds"] for r in d["reps"])
        rows.append(f'''            <tr>
              <td><span class="swatch {sut_class(d["sut"])}"></span>{disp(d["sut"])}<span class="info" data-tip="{tip(d)}">i</span></td>
              <td class="num mono">{fmt_s(d["medianWallSeconds"])}</td>
              <td class="num mono">{d["medianRowsPerSec"]:,.0f}</td>
              <td class="num mono">{walls[0]:.3f}&ndash;{walls[-1]:.3f}</td>
              <td class="num mono">{setup:.1f}s</td>
              <td class="num pass mono">{"PASS" if d["parityPass"] else "FAIL"}</td>
            </tr>''')
    return "\n".join(rows)

def endpoint_summary(d):
    source = d.get("source", {"engine": d["route"].split("-")[0], "provider": "testcontainers"})
    sink = d.get("sink", {"engine": d["route"].split("-")[1], "provider": "testcontainers"})
    source_label = f"/{source['label']}" if source.get("label") else ""
    sink_label = f"/{sink['label']}" if sink.get("label") else ""
    return (
        f"source {source['engine']}/{source['provider']}{source_label} · "
        f"sink {sink['engine']}/{sink['provider']}{sink_label}"
    )

CTR_HEAD = '''      <div class="tablewrap">
        <table>
          <thead>
            <tr>
              <th>System</th>
              <th>Container<span class="info" data-tip="The tool plus the source and sink databases.">i</span></th>
              <th class="num"><span class="hint" data-tip="CPU seconds during the load.">CPU</span></th>
              <th class="num"><span class="hint" data-tip="Highest sample during the load (100ms cadence).">Peak memory</span></th>
              <th class="num"><span class="hint" data-tip="Bytes received during the load.">Net in</span></th>
              <th class="num"><span class="hint end" data-tip="Bytes sent during the load.">Net out</span></th>
            </tr>
          </thead>
          <tbody>'''

RUNS_HEAD = '''      <div class="tablewrap">
        <table>
          <thead>
            <tr>
              <th>System</th>
              <th class="num"><span class="hint" data-tip="Median across runs. Only the load is timed.">Wall, median</span></th>
              <th class="num"><span class="hint" data-tip="Rows moved per second of wall time.">Rows/s</span></th>
              <th class="num"><span class="hint" data-tip="Fastest and slowest run.">Range</span></th>
              <th class="num"><span class="hint" data-tip="Untimed preparation before the load.">Setup</span></th>
              <th class="num"><span class="hint end" data-tip="Source and destination row counts must match on every table.">Parity</span></th>
            </tr>
          </thead>
          <tbody>'''

def panel(ds, route, route_runs, active):
    rows_total = route_runs[0]["rows"]
    wall = bar_rows([(d, d["medianWallSeconds"]) for d in route_runs], lambda v: fmt_s(v, 2))
    mem_items = [(d, tool_mem(d)) for d in route_runs]
    mem_items = [(d, v) for d, v in mem_items if v > 0]
    mem = bar_rows(sorted(mem_items, key=lambda x: x[1]), fmt_bytes, single=True)
    cpu = bar_rows(sorted([(d, tool_cpu(d)) for d in route_runs], key=lambda x: x[1]),
                   lambda v: fmt_s(v, 2), single=True)
    fn = ""
    reps = len(route_runs[0]["reps"])
    endpoints = html.escape(endpoint_summary(route_runs[0]))
    return f'''  <div class="panel{" active" if active else ""}" id="p-{ds}-{route}">
    <section>
      <h2>Wall time</h2>
      <div class="sectionnote">Median seconds to move {rows_total:,} rows {ROUTE_LABEL[route]}. Lower is better. {endpoints}.</div>
      <div class="chart">
{wall}
      </div>
    </section>

    <section>
      <h2>Peak memory</h2>
      <div class="sectionnote">The most memory the tool held during the load. A tool that runs several
      containers reports their sum. The native baseline finishes before the sampler can catch it at this scale.</div>
      <div class="chart single">
{mem}
      </div>
    </section>

    <section>
      <h2>CPU time</h2>
      <div class="sectionnote">CPU seconds used by the tool containers during the load. Remote database CPU is intentionally excluded.</div>
      <div class="chart single">
{cpu}
      </div>
    </section>

    <section>
      <h2>Containers</h2>
      <div class="sectionnote">The median run, split by sampled container. Remote endpoints are identified in the cohort metadata but have no Docker stats.</div>
{CTR_HEAD}
{containers_table(route_runs)}
          </tbody>
        </table>
      </div>
    </section>

    <section>
      <h2>Runs</h2>
      <div class="sectionnote">Medians of {reps} runs. Hover a name for the configuration.</div>
{RUNS_HEAD}
{runs_table(route_runs)}
          </tbody>
        </table>
      </div>
    </section>{fn}
  </div>'''

def main():
    parser = argparse.ArgumentParser(description="render one benchmark cohort")
    parser.add_argument("date")
    parser.add_argument("--cohort")
    args = parser.parse_args()
    paths, out_path, cohort = result_paths(args.date, args.cohort)
    data = load(paths)
    datasets = list(data)
    ds0 = datasets[0]
    runs = data[ds0]
    src = TEMPLATE.read_text()
    style = re.search(r"<style>(.*?)</style>", src, re.S).group(1)
    headrow = re.search(r'<div class="headrow">.*?</div>\s*</header>', src, re.S).group(0)
    headrow = headrow.rsplit("</header>", 1)[0].rstrip()

    measured = {r for ds in data for r in data[ds]}
    first_route = next(route for route in ROUTES if route in measured)
    route_names = {
        "pg-pg": "PG → PG", "pg-mysql": "PG → MySQL", "mysql-pg": "MySQL → PG",
        "mysql-mysql": "MySQL → MySQL", "pg-iceberg": "PG → Iceberg", "mysql-iceberg": "MySQL → Iceberg",
    }
    rtseg = '<div class="seg" id="rtseg">' + "".join(
        f'<button data-route="{route}" aria-pressed="{"true" if route == first_route else "false"}">{route_names[route]}</button>'
        for route in ROUTES if route in measured
    ) + "</div>"

    d0 = next(iter(runs.values()))[0]
    extra_css = """
  .bar.filament2, .swatch.filament2 { background: var(--sut-filament); }
  .bar.airbyte, .swatch.airbyte { background: #8a8a8a; }
  .bar.debezium, .swatch.debezium { background: #8a8a8a; }
  .bar.native { background: repeating-linear-gradient(-45deg, var(--sut-copy) 0 3px, transparent 3px 7px); }
  .swatch.native { background: var(--sut-copy); }
  td.lastgroup { border-bottom: none; }
  .name .hint, td .hint { border-bottom: none; }
"""
    panels = "\n\n".join(
        panel(ds, r, data[ds][r], ds == ds0 and r == next(rr for rr in ROUTES if rr in data[ds0]))
        for ds in datasets for r in ROUTES if r in data[ds])
    ds_buttons = "".join(
        f'<button data-ds="{ds}" aria-pressed="{"true" if ds == ds0 else "false"}">{dataset_label(ds)}</button>'
        for ds in datasets)
    avail = {ds: [r for r in ROUTES if r in data[ds]] for ds in datasets}
    rows_by_ds = {ds: f"{next(iter(data[ds].values()))[0]['rows']:,} rows" for ds in datasets}

    machine = html.escape(d0.get("machine") or "unlabeled machine")
    memory = f" · {fmt_bytes(d0['memoryBytes'])}" if d0.get("memoryBytes") else ""
    topology = html.escape(d0.get("topology", "legacy-local"))
    endpoints = endpoint_summary(d0) if len(measured) == 1 else "route-specific endpoints shown with each result"
    dirty = " · DIRTY WORKTREE" if d0.get("gitDirty") else ""

    page = f'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data Movement Benchmark &middot; {args.date} &middot; {html.escape(cohort)}</title>
<style>{style}{extra_css}</style>
</head>
<body>
<main>
  <header>
    <div class="headrow">
      <h1>Data Movement Benchmark</h1>
{headrow.split("<h1>Data Movement Benchmark</h1>")[1]}
  </header>

  <div class="intro">
    <dl class="facts">
      <dt>Workload</dt><dd class="wl">
        <div class="seg seg-sm" id="dsseg">{ds_buttons}</div>
        <span class="ctl-note" id="dsrows">{rows_by_ds[ds0]}</span></dd>
      <dt>Machine</dt><dd>{machine} &middot; {d0.get("cpus", "?")} vCPU{memory} &middot; {d0["os"]}/{d0["arch"]}</dd>
      <dt>Topology</dt><dd>{topology} &middot; {html.escape(endpoints)}</dd>
      <dt>Cohort</dt><dd>{html.escape(cohort)} &middot; {html.escape(d0.get("gitCommit", "unknown commit")[:12])}{dirty}</dd>
      <dt>Runs</dt><dd>Medians of clean, repeated runs; the trigger-to-completion load window is timed</dd>
      <dt>Correctness</dt><dd>Source and destination row counts must match; invalid runs are not written</dd>
    </dl>
  </div>

  <h2 class="pagehead">Results</h2>

  <div class="controls">
{rtseg}
    </div>

{panels}
</main>
<script>
  const AVAIL = {json.dumps(avail)};
  const ROWS = {json.dumps(rows_by_ds)};
  let ds = "{ds0}", route = AVAIL[ds][0];
  const dsButtons = document.querySelectorAll("#dsseg button");
  const rtButtons = document.querySelectorAll("#rtseg button[data-route]");
  function render() {{
    if (!AVAIL[ds].includes(route)) route = AVAIL[ds][0];
    dsButtons.forEach(b => b.setAttribute("aria-pressed", b.dataset.ds === ds ? "true" : "false"));
    rtButtons.forEach(b => {{
      b.disabled = !AVAIL[ds].includes(b.dataset.route);
      b.setAttribute("aria-pressed", b.dataset.route === route ? "true" : "false");
    }});
    document.getElementById("dsrows").textContent = ROWS[ds];
    document.querySelectorAll(".panel").forEach(p =>
      p.classList.toggle("active", p.id === "p-" + ds + "-" + route));
  }}
  dsButtons.forEach(b => b.addEventListener("click", () => {{ ds = b.dataset.ds; render(); }}));
  rtButtons.forEach(b => b.addEventListener("click", () => {{ route = b.dataset.route; render(); }}));
  render();
</script>
</body>
</html>
'''
    out_path.write_text(page)
    print("wrote", out_path, len(page), "bytes")

if __name__ == "__main__":
    main()
