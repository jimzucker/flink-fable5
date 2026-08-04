#!/usr/bin/env python3
"""Executive briefing deck for the flink-demo project."""
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.chart.data import CategoryChartData
from pptx.enum.chart import XL_CHART_TYPE, XL_LABEL_POSITION

ACCENT = RGBColor(0x2A, 0x78, 0xD6)   # validated palette blue
DARK = RGBColor(0x0B, 0x0B, 0x0B)
GRAY = RGBColor(0x52, 0x51, 0x4E)
GREEN = RGBColor(0x00, 0x83, 0x00)
AMBER = RGBColor(0xC9, 0x85, 0x00)
LIGHT = RGBColor(0xF0, 0xEF, 0xEC)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)

prs = Presentation()
prs.slide_width = Inches(13.333)
prs.slide_height = Inches(7.5)
BLANK = prs.slide_layouts[6]


def slide():
    return prs.slides.add_slide(BLANK)


def textbox(s, x, y, w, h):
    box = s.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    box.text_frame.word_wrap = True
    return box


def title(s, text, sub=None):
    box = textbox(s, 0.6, 0.35, 12.1, 1.0)
    p = box.text_frame.paragraphs[0]
    r = p.add_run(); r.text = text
    r.font.size = Pt(30); r.font.bold = True; r.font.color.rgb = DARK
    bar = s.shapes.add_shape(1, Inches(0.65), Inches(1.15), Inches(2.2), Pt(4))
    bar.fill.solid(); bar.fill.fore_color.rgb = ACCENT; bar.line.fill.background()
    if sub:
        sb = textbox(s, 0.6, 1.25, 12.1, 0.5)
        p = sb.text_frame.paragraphs[0]
        r = p.add_run(); r.text = sub
        r.font.size = Pt(15); r.font.color.rgb = GRAY


def bullets(s, items, x=0.7, y=1.9, w=11.9, size=18, gap=8):
    box = textbox(s, x, y, w, 5.2)
    tf = box.text_frame
    first = True
    for item in items:
        if isinstance(item, tuple):
            text, level, color, bold = item
        else:
            text, level, color, bold = item, 0, DARK, False
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        p.level = level
        p.space_after = Pt(gap)
        r = p.add_run()
        r.text = ("• " if level == 0 else "– ") + text
        r.font.size = Pt(size if level == 0 else size - 2)
        r.font.color.rgb = color
        r.font.bold = bold
    return box


def table(s, rows, x, y, w, col_widths, size=14, header=True):
    shape = s.shapes.add_table(len(rows), len(rows[0]), Inches(x), Inches(y),
                               Inches(w), Inches(0.4 * len(rows)))
    t = shape.table
    for i, cw in enumerate(col_widths):
        t.columns[i].width = Inches(cw)
    for ri, row in enumerate(rows):
        for ci, val in enumerate(row):
            cell = t.cell(ri, ci)
            cell.margin_top = cell.margin_bottom = Emu(45720)
            tf = cell.text_frame; tf.word_wrap = True
            p = tf.paragraphs[0]
            color = DARK
            text = val
            if isinstance(val, tuple):
                text, color = val
            r = p.add_run(); r.text = str(text)
            r.font.size = Pt(size)
            if header and ri == 0:
                r.font.bold = True; r.font.color.rgb = WHITE
                cell.fill.solid(); cell.fill.fore_color.rgb = ACCENT
            else:
                r.font.color.rgb = color
                cell.fill.solid()
                cell.fill.fore_color.rgb = WHITE if ri % 2 else LIGHT
    return shape


def box(s, x, y, w, h, text, fill=LIGHT, text_color=DARK, size=13, bold_first=True):
    shp = s.shapes.add_shape(5, Inches(x), Inches(y), Inches(w), Inches(h))  # rounded rect
    shp.fill.solid(); shp.fill.fore_color.rgb = fill
    shp.line.color.rgb = ACCENT; shp.line.width = Pt(1)
    tf = shp.text_frame; tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    lines = text.split("\n")
    for i, ln in enumerate(lines):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = PP_ALIGN.CENTER
        r = p.add_run(); r.text = ln
        r.font.size = Pt(size + (1 if (i == 0 and bold_first) else -1))
        r.font.bold = bold_first and i == 0
        r.font.color.rgb = text_color
    return shp


def arrow(s, x, y, w=0.4):
    a = s.shapes.add_shape(33, Inches(x), Inches(y), Inches(w), Inches(0.25))  # right arrow
    a.fill.solid(); a.fill.fore_color.rgb = GRAY; a.line.fill.background()


# ---------------- Slide 1: Title ----------------
s = slide()
r = s.shapes.add_shape(1, 0, 0, prs.slide_width, Inches(0.18))
r.fill.solid(); r.fill.fore_color.rgb = ACCENT; r.line.fill.background()
b = textbox(s, 0.9, 2.3, 11.5, 1.4)
p = b.text_frame.paragraphs[0]
run = p.add_run(); run.text = "Real-Time Positions & Market Value"
run.font.size = Pt(44); run.font.bold = True; run.font.color.rgb = DARK
p2 = b.text_frame.add_paragraph()
run = p2.add_run(); run.text = "A streaming trading pipeline on Apache Flink — demo project results"
run.font.size = Pt(22); run.font.color.rgb = GRAY
b2 = textbox(s, 0.9, 5.6, 11.5, 1.2)
for i, line in enumerate(["Executive briefing  ·  August 2026",
                          "Built in one day with Claude Code  ·  github.com/jimzucker/flink-fable5"]):
    p = b2.text_frame.paragraphs[0] if i == 0 else b2.text_frame.add_paragraph()
    run = p.add_run(); run.text = line
    run.font.size = Pt(15); run.font.color.rgb = GRAY

# ---------------- Slide 2: The ask ----------------
s = slide()
title(s, "The ask", "From Requirements.txt — a production-shaped proof, not a toy")
bullets(s, [
    ("Consume block trades and ticking prices from Kafka; produce live positions and market values by account and by ticker", 0, DARK, False),
    ("Runs on a laptop and deploys to AWS — same application, configuration-only differences", 0, DARK, False),
    ("Scales linearly with CPU; scaling controlled by configuration, never a rebuild", 0, DARK, False),
    ("Handles duplicates; results deterministic, complete, reproducible — and explainable to the team", 0, DARK, False),
    ("Proven under load: 1,000 orders/sec, and extreme price values, without performance impact", 0, DARK, False),
], size=20, gap=14)

# ---------------- Slide 3: What we built ----------------
s = slide()
title(s, "What we built", "Deterministic streaming pipeline — every number exact, every operator measured")
box(s, 0.6, 2.0, 2.1, 1.5, "Kafka\ntrades\nprices")
arrow(s, 2.8, 2.65)
box(s, 3.3, 1.65, 2.3, 0.95, "Dedup\ntrade-id, state+TTL")
box(s, 3.3, 2.85, 2.3, 0.95, "Exact prices\nlong cents, no floats")
arrow(s, 5.7, 2.65)
box(s, 6.2, 1.65, 2.5, 0.95, "Positions\nby account+ticker / ticker")
box(s, 6.2, 2.85, 2.5, 0.95, "Market value\nposition × latest price")
arrow(s, 8.8, 2.65)
box(s, 9.3, 2.0, 2.0, 1.5, "4 output\nstreams\n(Kafka)")
box(s, 3.3, 4.15, 5.4, 0.8, "Observability: Prometheus + Grafana, 12 panels\nrecords/sec, KB/sec, duplicates, backpressure — per operator", fill=WHITE)
bullets(s, [
    ("Apache Flink 1.20 (Java 17) — industry-standard stream processing; RocksDB state, checkpointing", 0, DARK, False),
    ("All money math exact (long cents + BigDecimal): correct at any price magnitude, by construction", 0, DARK, False),
    ("Every behavior is a config knob — rates, parallelism, dedup TTL: zero rebuilds to change anything", 0, DARK, False),
], y=5.35, size=16, gap=6)

# ---------------- Slide 4: Delivery discipline ----------------
s = slide()
title(s, "How it was delivered", "Eight phases — each ended in something reviewable and running")
phases = [
    ("1. Design", "Plan + architecture,\nreviewed first"),
    ("2. Skeleton", "End-to-end on\nlaptop, day one"),
    ("3. Full calcs", "All 4 outputs +\ndashboard, live"),
    ("4. Correctness", "18 tests + indep.\nvalidation, green"),
    ("5. AWS IaC", "Terraform validated,\nready to apply"),
    ("6. Load tests", "Both perf cases\npassed, measured"),
    ("7. Review find", "Price-storm flaw:\nproven, fixed, verified"),
    ("8. AWS proof", "Deployed, all tests\ngreen, ladder measured"),
]
for i, (h, d) in enumerate(phases):
    x = 0.4 + i * 1.585
    box(s, x, 2.2, 1.48, 1.6, f"{h}\n{d}", fill=LIGHT if i < 6 else WHITE, size=10)
bullets(s, [
    ("Nothing advanced unverified: every phase closed with the system running and measured", 0, DARK, True),
    ("Full audit trail in git: one squash commit per phase, tagged Phase-2 … Phase-6, branches preserved", 0, DARK, False),
    ("Phase 7 was unplanned: a human review comment predicted the price-storm bottleneck — proven, fixed and re-verified the same day", 0, DARK, True),
    ("The seven engineering prompts that drove the build are recorded in the repo (prompts/)", 0, DARK, False),
], y=4.6, size=17, gap=10)

# ---------------- Slide 5: Correctness ----------------
s = slide()
title(s, "How we know the numbers are right", "Three independent layers of proof")
bullets(s, [
    ("18 unit tests against the real operators — including a golden dataset with hand-computed expected results", 0, DARK, False),
    ("Duplicate-injection proven: first occurrence wins; replays absorbed silently", 1, GRAY, False),
    ("Extreme-price test: 999,999 shares × $10 trillion — exact to the last digit, no overflow", 1, GRAY, False),
    ("Independent validation: a script re-derives every output from the raw input topics and compares exactly", 0, DARK, False),
    ("Live run: 20,210 trades, 959 duplicates dropped, all 50 positions and every market value exact to the penny — 6/6 checks PASS", 1, GREEN, True),
    ("Completeness invariant holds: per-account positions always sum to the ticker position", 0, DARK, False),
    ("Deterministic: same inputs → identical outputs, proven by test; generator fully seeded", 0, DARK, False),
], size=18, gap=10)

# ---------------- Slide 6: Performance ----------------
s = slide()
title(s, "Load test results — both required cases pass", "Config-only changes; the application was never rebuilt")
table(s, [
    ["", "Baseline (10/sec)", "Case 1: 1,000 orders/sec", "Case 2: $10 trillion price"],
    ["Throughput", "10/sec", ("sustained 1,000/sec", GREEN), ("sustained 1,000/sec", GREEN)],
    ["CPU (busiest task)", "0.9%", "5.2%", "4.9%"],
    ["Backpressure / lag", "none", ("none", GREEN), ("none", GREEN)],
    ["Latency (p50)", "100 ms", ("96 ms — unchanged", GREEN), "118 ms — within noise"],
    ["Verdict", "reference", ("PASS", GREEN), ("PASS — math exact to 19 digits", GREEN)],
], 0.7, 2.0, 11.9, [2.3, 2.9, 3.5, 3.2], size=15)
bullets(s, [
    ("100× the order rate moved latency not at all — the pipeline runs at ~5% capacity at 1,000/sec (~14× headroom)", 0, DARK, True),
    ("Price magnitude is data, not work: the extreme-price case cannot slow the order path by design", 0, DARK, False),
    ("Review-driven Case 3: 10,000 price ticks/sec saturated the naive design; conflated re-valuation (config: 250 ms) gives 240x less work, zero backpressure, flat order latency", 0, GREEN, True),
], y=5.25, size=15, gap=6)

# ---------------- Slide 7: Scalability ----------------
s = slide()
title(s, "Scaling is linear — and configuration-only", "Measured on AWS under sustained overload (Managed Flink KPUs)")
chart_data = CategoryChartData()
chart_data.categories = ["P=2 (2 KPUs)", "P=4 (4 KPUs)", "P=8 (8 KPUs)"]
chart_data.add_series("Processed msgs/sec at saturation", (16500, 52000, 97000))
gframe = s.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED,
                            Inches(0.7), Inches(1.8), Inches(6.6), Inches(4.6), chart_data)
chart = gframe.chart
chart.has_legend = False
plot = chart.plots[0]
plot.has_data_labels = True
plot.data_labels.number_format = '#,##0'
plot.data_labels.number_format_is_linked = False
plot.data_labels.position = XL_LABEL_POSITION.OUTSIDE_END
plot.data_labels.font.size = Pt(13)
plot.data_labels.font.color.rgb = DARK
series = plot.series[0]
series.format.fill.solid()
series.format.fill.fore_color.rgb = ACCENT
chart.category_axis.tick_labels.font.size = Pt(13)
chart.value_axis.tick_labels.font.size = Pt(11)
chart.value_axis.has_major_gridlines = True
bullets(s, [
    ("Each doubling of parallelism ~doubles throughput — one Terraform variable, zero job restarts", 0, GREEN, True),
    ("Capacity model: ~12k msgs/sec per KPU (~$0.12/hr each)", 0, DARK, False),
    ("The 110k msgs/sec stress load = a ~P=10 dial setting", 0, DARK, False),
    ("Next 3-5x identified: binary serialization (Avro) instead of JSON — before buying more KPUs", 0, DARK, False),
    ("Laptop result matched: P=1->2 exactly 2.0x", 0, GRAY, False),
], x=7.6, y=2.2, w=5.3, size=15, gap=10)

# ---------------- Slide 7b: The ladder, as measured ----------------
s = slide()
title(s, "The ladder, as measured", "CloudWatch, per-subtask throughput and saturation — AWS, Managed Flink")
s.shapes.add_picture("/Users/jimzucker/code/GitHub/flink-fable5/docs/images/aws-ladder-throughput.png",
                     Inches(0.5), Inches(1.7), Inches(12.3), Inches(2.7))
s.shapes.add_picture("/Users/jimzucker/code/GitHub/flink-fable5/docs/images/aws-busy-backpressure.png",
                     Inches(0.5), Inches(4.5), Inches(12.3), Inches(2.7))


# ---------------- Slide 7c: Second cloud — Confluent ----------------
s = slide()
title(s, "Two clouds, one test suite — the measured scoreboard", "Same requirements, same data, same five correctness checks; deployable configs only")
table(s, [
    ["", "AWS (Managed Flink + MSK)", "Confluent Cloud (Flink SQL)"],
    ["Correctness - 5 independent checks", "All pass, exact to the cent", "All pass, exact to the cent"],
    ["End-to-end latency (live)", "p50 267 ms  /  p99 495 ms", "p50 1.8 s  /  p99 2.1 s (SQL tuned for latency)"],
    ["Full pipeline throughput", "435,000 msgs/sec", "127,000 msgs/sec"],
    ["Sustained 28 min", "232,705/sec, deviation 30/sec", "360,900/sec, deviation 63,900/sec"],
    ["Cost at equal work", "~ $2.39/hr all-in", "~ $3.36/hr + usage"],
    ["Code to build it", "~2,000 lines of Java", "~200 lines of SQL"],
    ["What you operate", "a jar, an image, a VPC", "statements - nothing to deploy"],
    ["Rate-limit an output (CR-1)", "config change - 0.96/key/sec vs 1.0 cap", "NOT EXPRESSIBLE for updating aggregations"],
], 0.7, 1.8, 12.0, [4.2, 3.9, 3.9], size=13)
bullets(s, [
    ("AWS is faster, cheaper and steadier: one job passes records between operators in memory", 0, GREEN, True),
    ("Confluent is ~10x less code and nothing to deploy - but the SQL must be ONE fused statement set; writing it as chained statements costs 15x latency (26.7s -> 1.8s when fixed)", 0, DARK, False),
    ("The deciding factor was not speed: a plain requirement - do not update the screen faster than a human can read - is one config line on DataStream and has no supported construct in Confluent SQL", 0, ACCENT, True),
], y=5.9, size=15, gap=9)

# ---------------- Slide 8: Production path ----------------
s = slide()
title(s, "Path to production — AWS ready today", "Full infrastructure-as-code, validated; one command to deploy")
bullets(s, [
    ("Terraform stack complete: MSK Serverless (Kafka) + Amazon Managed Service for Apache Flink + generator on Fargate + CloudWatch dashboard", 0, DARK, False),
    ("The exact same application jar runs on AWS — every environment difference is configuration", 0, DARK, True),
    ("Security by default: private VPC, IAM auth to Kafka, least-privilege roles", 0, DARK, False),
    ("Operations runbook: deploy, in-place code update, config-only rescale, teardown", 0, DARK, False),
    ("Demo cost ≈ $1/hour while running; destroyed when not in use", 0, DARK, False),
    ("Status: DEPLOYED and load-tested on AWS — all cases green, scaling ladder measured, then torn down cleanly", 0, GREEN, True),
], size=18, gap=12)

# ---------------- Slide 9: Build efficiency ----------------
s = slide()
title(s, "What it took to build", "AI-assisted engineering with review gates")
stats = [("1 day + epilogue", "requirements to a\ntwo-cloud proof"), ("9 prompts", "drove all nine\nphases of work"),
         ("≈ $515", "metered AI compute\n(flat-rate in practice)"), ("26 finds", "7 bugs, 1 design flaw,\n18 deploy/cleanup gotchas")]
for i, (big, small) in enumerate(stats):
    x = 0.7 + i * 3.1
    shp = s.shapes.add_shape(5, Inches(x), Inches(2.1), Inches(2.8), Inches(2.0))
    shp.fill.solid(); shp.fill.fore_color.rgb = LIGHT
    shp.line.color.rgb = ACCENT; shp.line.width = Pt(1)
    tf = shp.text_frame; tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    p = tf.paragraphs[0]; p.alignment = PP_ALIGN.CENTER
    r = p.add_run(); r.text = big; r.font.size = Pt(34); r.font.bold = True; r.font.color.rgb = ACCENT
    p2 = tf.add_paragraph(); p2.alignment = PP_ALIGN.CENTER
    for j, ln in enumerate(small.split("\n")):
        pp = p2 if j == 0 else tf.add_paragraph()
        pp.alignment = PP_ALIGN.CENTER
        r = pp.add_run(); r.text = ln; r.font.size = Pt(13); r.font.color.rgb = GRAY
bullets(s, [
    ("Quality came from the process: plan first, review each outcome, make the system prove itself", 0, DARK, True),
    ("Bugs caught included the kind that bite in production: dependency clashes, silent metric mismatches, replay semantics", 0, DARK, False),
    ("Every prompt, decision, and result is recorded in the repository — fully auditable", 0, DARK, False),
], y=4.7, size=17, gap=10)

# ---------------- Slide 10: Recommendations ----------------
s = slide()
title(s, "Recommendations & next steps")
bullets(s, [
    ("Recommend AWS Managed Flink for this product: it is faster, cheaper at equal work, and the only one of the two that can satisfy the output-cadence requirement (CR-1)", 0, DARK, True),
    ("Volume and stability proven on both, same method: AWS 757k msgs/sec peak, 28 min at 232,705/sec with deviation of 30; Confluent 386k peak single-statement", 0, DARK, False),
    ("Team walkthrough: live dashboard + 'explain the numbers' script are ready (docs/PERF_RESULTS.md)", 0, DARK, False),
    ("Candidate hardening for production: schema registry, DLQ for malformed records, autoscaling policy, CI pipeline", 0, DARK, False),
    ("Adopt the delivery pattern (phased outcomes + self-verification) for the next AI-assisted build", 0, DARK, False),
], size=19, gap=14)
b = textbox(s, 0.7, 6.3, 12.0, 0.6)
p = b.text_frame.paragraphs[0]
r = p.add_run(); r.text = "Repo: github.com/jimzucker/flink-fable5  ·  All results reproducible: make test · make validate · perf scripts"
r.font.size = Pt(13); r.font.color.rgb = GRAY

out = "/Users/jimzucker/code/GitHub/flink-fable5/docs/flink-demo-exec-briefing.pptx"
prs.save(out)
print("wrote", out)
