import glob
import html
import json
import re
import sys
from pathlib import Path

DATE = sys.argv[1]
RESULTS = Path(__file__).resolve().parent.parent
TEMPLATE = Path(__file__).resolve().parent / "template.html"
OUT = RESULTS / DATE / "index.html"

ROUTES = ["pg-pg", "pg-mysql", "mysql-pg", "mysql-mysql"]
ROUTE_LABEL = {
    "pg-pg": "from Postgres 16 to Postgres 16",
    "pg-mysql": "from Postgres 16 to MySQL 8.4",
    "mysql-pg": "from MySQL 8.4 to Postgres 16",
    "mysql-mysql": "from MySQL 8.4 to MySQL 8.4",
}
# display order within charts is by median wall ascending; table order matches.
BASELINE = "native"  # hatched bar

def fmt_bytes(n):
    if n >= 1e9: return f"{n/1e9:.2f} GB"
    if n >= 1e6: return f"{n/1e6:.0f} MB"
    return f"{n/1e3:.0f} KB"

def fmt_s(v, prec=3):
    return f"{v:.{prec}f}s"

def sf_of(ds):
    return float(ds.split("sf")[-1])

def load():
    data = {}
    for p in sorted(glob.glob(str(RESULTS / DATE / "*" / "*.json"))):
        d = json.load(open(p))
        data.setdefault(d["dataset"], {}).setdefault(d["route"], []).append(d)
    for ds in data:
        for route in data[ds]:
            data[ds][route].sort(key=lambda d: d["medianWallSeconds"])
    return dict(sorted(data.items(), key=lambda kv: -sf_of(kv[0])))

def median_rep(d):
    reps = sorted(d["reps"], key=lambda r: r["wallSeconds"])
    return reps[len(reps) // 2]

def tool_roles(rep):
    return [u for u in rep.get("resources", []) if u["role"] not in ("source", "sink")]

def db_roles(rep):
    return [u for u in rep.get("resources", []) if u["role"] in ("source", "sink")]

def tool_mem(d):
    return sum(u["peakMemBytes"] for u in tool_roles(median_rep(d)))

def total_cpu(d):
    return sum(u["cpuSeconds"] for u in median_rep(d).get("resources", []))

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
        setup = sorted(r["setupSeconds"] for r in d["reps"])[len(d["reps"]) // 2]
        rows.append(f'''            <tr>
              <td><span class="swatch {sut_class(d["sut"])}"></span>{disp(d["sut"])}<span class="info" data-tip="{tip(d)}">i</span></td>
              <td class="num mono">{fmt_s(d["medianWallSeconds"])}</td>
              <td class="num mono">{d["medianRowsPerSec"]:,.0f}</td>
              <td class="num mono">{walls[0]:.3f}&ndash;{walls[-1]:.3f}</td>
              <td class="num mono">{setup:.1f}s</td>
              <td class="num pass mono">{"PASS" if d["parityPass"] else "FAIL"}</td>
            </tr>''')
    return "\n".join(rows)

CTR_HEAD = '''      <div class="tablewrap">
        <table>
          <thead>
            <tr>
              <th>System</th>
              <th>Container<span class="info" data-tip="The tool plus the source and sink databases.">i</span></th>
              <th class="num"><span class="hint" data-tip="CPU seconds during the load.">CPU</span></th>
              <th class="num"><span class="hint" data-tip="Highest sample during the load (500ms cadence).">Peak memory</span></th>
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
              <th class="num"><span class="hint end" data-tip="Row counts must match on every table.">Parity</span></th>
            </tr>
          </thead>
          <tbody>'''

def panel(ds, route, route_runs, active):
    rows_total = route_runs[0]["rows"]
    wall = bar_rows([(d, d["medianWallSeconds"]) for d in route_runs], lambda v: fmt_s(v, 2))
    mem_items = [(d, tool_mem(d)) for d in route_runs]
    mem_items = [(d, v) for d, v in mem_items if v > 0]
    mem = bar_rows(sorted(mem_items, key=lambda x: x[1]), fmt_bytes, single=True)
    cpu = bar_rows(sorted([(d, total_cpu(d)) for d in route_runs], key=lambda x: x[1]),
                   lambda v: fmt_s(v, 2), single=True)
    fn = ""
    reps = len(route_runs[0]["reps"])
    return f'''  <div class="panel{" active" if active else ""}" id="p-{ds}-{route}">
    <section>
      <h2>Wall time</h2>
      <div class="sectionnote">Median seconds to move {rows_total:,} rows {ROUTE_LABEL[route]}. Lower is better.</div>
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
      <div class="sectionnote">CPU seconds across tool, source, and sink during the load.</div>
      <div class="chart single">
{cpu}
      </div>
    </section>

    <section>
      <h2>Containers</h2>
      <div class="sectionnote">The median run, split by container: the tool, the source, and the sink.</div>
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
    data = load()
    datasets = list(data)
    ds0 = datasets[0]
    runs = data[ds0]
    src = TEMPLATE.read_text()
    style = re.search(r"<style>(.*?)</style>", src, re.S).group(1)
    headrow = re.search(r'<div class="headrow">.*?</div>\s*</header>', src, re.S).group(0)
    headrow = headrow.rsplit("</header>", 1)[0].rstrip()

    rtseg = re.search(r'<div class="seg" id="rtseg">.*?</div>\s*</div>\s*\n\s*<div class="panel active"', src, re.S).group(0)
    rtseg = rtseg.rsplit("</div>", 2)[0]
    btns = re.findall(r"<button.*?</button>", rtseg, re.S)
    assert len(btns) == 4, len(btns)
    measured = {r for ds in data for r in data[ds]}
    for i, route in enumerate(ROUTES):
        if route in measured:
            new = btns[i].replace('<button aria-pressed="true">', "<button>") \
                         .replace('<button disabled title="not yet measured">', "<button>")
            new = new.replace("<button>", f'<button data-route="{route}" aria-pressed="{"true" if i == 0 else "false"}">', 1)
            rtseg = rtseg.replace(btns[i], new)

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
        f'<button data-ds="{ds}" aria-pressed="{"true" if ds == ds0 else "false"}">TPC-H {ds.split("-")[-1]}</button>'
        for ds in datasets)
    avail = {ds: [r for r in ROUTES if r in data[ds]] for ds in datasets}
    rows_by_ds = {ds: f"{next(iter(data[ds].values()))[0]['rows']:,} rows" for ds in datasets}

    page = f'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data Movement Benchmark &middot; {DATE}</title>
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
      <dt>Machine</dt><dd>AWS <a href="https://aws.amazon.com/ec2/instance-types/m7i/" target="_blank" rel="noopener">m7i.2xlarge</a> &middot; 8 vCPU &middot; {d0["os"]}/{d0["arch"]}</dd>
      <dt>Runs</dt><dd>Medians of clean, repeated runs; only the load is timed</dd>
      <dt>Correctness</dt><dd>Row counts checked on every table; wrong data gets no results</dd>
    </dl>
  </div>

  <h2 class="pagehead">Results</h2>

  <div class="controls">
{rtseg}
    </div>
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
    OUT.write_text(page)
    print("wrote", OUT, len(page), "bytes")

main()
