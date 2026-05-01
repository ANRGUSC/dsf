"""
DSF lab-meeting presentation, built on top of the ANRG template.

Run:
    /home/anrg/dsf/.venv-presentation/bin/python presentation/build_deck.py

Output:
    presentation/dsf-talk.pptx

Layout strategy
---------------
We open the ANRG template, drop its example slides, and add new slides
using the template's master layouts so branding/fonts/slide numbers
propagate from the master.  Specialty diagram slides use TITLE_ONLY
plus custom shapes.

Slide dimensions in this template are 10.000 x 5.625 inches.
"""

from pathlib import Path
from copy import deepcopy

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.oxml.ns import qn

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT       = Path("/home/anrg/dsf")
TEMPLATE   = ROOT / "presentation" / "ANRG_presentation_template.pptx"
OUT_PATH   = ROOT / "presentation" / "dsf-talk.pptx"

# Figures
F = ROOT / "eval/network-aware/results/archive-matrix-v2/figures"
FIG_BANDWIDTH_MATRIX = F / "bandwidth-matrix.png"
FIG_MAKESPAN_DIST    = F / "makespan-distribution.png"
FIG_MAKESPAN_CONV    = F / "makespan-convergence.png"
FIG_INFER_PLACEMENT  = F / "iobt-infer-placement.png"
FIG_PRED_SCATTER     = F / "prediction-scatter.png"
FIG_CDAG_LATENCY     = ROOT / "eval/network-aware/archive-v1/figures/cdag-latency-throughput.png"
FIG_SCALABILITY      = ROOT / "eval/scalability/figures/scalability-combined.png"

# Layout indices (looked up from template)
L_TITLE        = 0
L_SECTION      = 1
L_TITLE_BODY   = 2
L_TITLE_2COL   = 3
L_TITLE_ONLY   = 4
L_SEC_DESC     = 7
L_BLANK        = 10

# Colours that match the ANRG template's palette
ACCENT  = RGBColor(0xC8, 0x5A, 0x17)   # USC-ish warm orange
NAVY    = RGBColor(0x1F, 0x3A, 0x68)   # readable navy for callouts
INK     = RGBColor(0x1A, 0x1A, 0x1A)
MUTED   = RGBColor(0x55, 0x55, 0x55)
SUBTLE  = RGBColor(0x88, 0x88, 0x88)
CARD_BG = RGBColor(0xF3, 0xF5, 0xF8)
CARD_BD = RGBColor(0xCF, 0xD7, 0xE0)
WHITE   = RGBColor(0xFF, 0xFF, 0xFF)

CODE_FONT = "Consolas"


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------
def clear_slides(prs):
    """Remove every slide reference from the presentation, leaving masters intact."""
    sldIdLst = prs.slides._sldIdLst
    rIds = [s.get(qn("r:id")) for s in list(sldIdLst)]
    for sldId in list(sldIdLst):
        sldIdLst.remove(sldId)
    for rId in rIds:
        try:
            prs.part.drop_rel(rId)
        except (KeyError, AttributeError):
            pass


def find_placeholder(slide, idx):
    for ph in slide.placeholders:
        if ph.placeholder_format.idx == idx:
            return ph
    return None


def style_run(run, *, size=None, bold=None, color=None, italic=None,
              font=None, underline=None):
    if size is not None:
        run.font.size = Pt(size)
    if bold is not None:
        run.font.bold = bold
    if italic is not None:
        run.font.italic = italic
    if font is not None:
        run.font.name = font
    if underline is not None:
        run.font.underline = underline
    if color is not None:
        run.font.color.rgb = color


def set_title(slide, text):
    ph = find_placeholder(slide, 0)
    if ph is None:
        return
    ph.text_frame.text = text


def set_subtitle(slide, text):
    ph = find_placeholder(slide, 1)
    if ph is None:
        return
    # Some layouts use ph idx=1 as BODY, some as SUBTITLE; either accepts text.
    ph.text_frame.text = text


def fill_body(ph, items, *, default_size=14, code_font=False):
    """
    Fill a body placeholder with bullet items.

    items is a list of either:
      - str
      - (level:int, text:str)
      - (level:int, text:str, dict-of-styles)

    The first paragraph is reused so we don't get a leading blank line.
    """
    tf = ph.text_frame
    tf.word_wrap = True
    # wipe existing paragraphs (placeholder usually has 1 default)
    for p in list(tf.paragraphs)[1:]:
        p._p.getparent().remove(p._p)
    # reset first paragraph
    p0 = tf.paragraphs[0]
    for r in list(p0.runs):
        r._r.getparent().remove(r._r)
    p0.text = ""

    first = True
    for item in items:
        if isinstance(item, str):
            level, text, style = 0, item, {}
        elif len(item) == 2:
            level, text = item
            style = {}
        else:
            level, text, style = item

        p = p0 if first else tf.add_paragraph()
        first = False
        p.level = level
        run = p.add_run()
        run.text = text
        size = style.get("size", default_size)
        style_run(run,
                  size=size,
                  bold=style.get("bold"),
                  italic=style.get("italic"),
                  color=style.get("color"),
                  font=style.get("font", CODE_FONT if code_font else None))


def add_textbox(slide, x, y, w, h, *, anchor=MSO_ANCHOR.TOP):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    tf.margin_left = Emu(0); tf.margin_right = Emu(0)
    tf.margin_top  = Emu(0); tf.margin_bottom = Emu(0)
    tf.vertical_anchor = anchor
    # remove the default empty paragraph's run text
    return tb, tf


def add_para(tf, text, *, size=12, bold=False, color=INK, italic=False,
             font=None, align=PP_ALIGN.LEFT, first=False, space_after=2):
    p = tf.paragraphs[0] if first else tf.add_paragraph()
    p.alignment = align
    p.space_after = Pt(space_after)
    if first and p.runs:
        run = p.runs[0]
        run.text = text
    else:
        # clear any default empty run on first
        if first:
            for r in list(p.runs):
                r._r.getparent().remove(r._r)
        run = p.add_run()
        run.text = text
    style_run(run, size=size, bold=bold, italic=italic, color=color, font=font)
    return p


def add_card(slide, x, y, w, h, *, fill=CARD_BG, border=CARD_BD):
    sh = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h)
    sh.adjustments[0] = 0.06
    sh.fill.solid(); sh.fill.fore_color.rgb = fill
    sh.line.color.rgb = border
    sh.line.width = Pt(0.5)
    return sh


def add_image_or_placeholder(slide, fig_path, x, y, w, h, label):
    if fig_path and Path(fig_path).exists():
        slide.shapes.add_picture(str(fig_path), x, y, width=w, height=h)
        return
    box = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, x, y, w, h)
    box.fill.solid(); box.fill.fore_color.rgb = RGBColor(0xFA, 0xFA, 0xFA)
    box.line.color.rgb = SUBTLE; box.line.width = Pt(1.0)
    box.line.dash_style = 7
    tb, tf = add_textbox(slide, x, y, w, h, anchor=MSO_ANCHOR.MIDDLE)
    add_para(tf, f"[ FIGURE: {label} ]",
             size=11, italic=True, color=MUTED, align=PP_ALIGN.CENTER, first=True)


def add_speaker_notes(slide, text):
    slide.notes_slide.notes_text_frame.text = text


# ---------------------------------------------------------------------------
# Slide builders.
# Every builder takes (prs) and returns the new slide.
# ---------------------------------------------------------------------------
def slide_title(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE])
    set_title(s,
        "DSF: A Network-Aware DAG Scheduling Framework "
        "for Heterogeneous Edge Clusters")

    sub = find_placeholder(s, 1)
    sub.text_frame.text = ""
    tf = sub.text_frame
    add_para(tf, "Mohammadali Khodabandehlou",
             size=14, bold=True, color=INK, first=True, space_after=2)
    add_para(tf, "mohammadali.khodabandelou@gmail.com",
             size=12, color=MUTED, space_after=6)
    add_para(tf, "Autonomous Networks Research Group",
             size=12, color=INK, space_after=0)
    add_para(tf, "Viterbi School of Engineering, "
                  "University of Southern California",
             size=12, color=INK, space_after=0)

    # Date / venue line, bottom-left
    tb, tf = add_textbox(s, Inches(0.34), Inches(5.05),
                         Inches(6.0), Inches(0.3))
    add_para(tf, "April 28, 2026     ·     ANRG Group Meeting",
             size=11, italic=True, color=MUTED, first=True)

    add_speaker_notes(s,
        "Title slide. Brief greeting; ~35 minutes of material plus Q&A. "
        "I'll outline the motivation, system, scheduling, and what we measured.")
    return s


def slide_outline(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Outline")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Background and problem statement"),
        (0, "Limitations of existing DAG/streaming frameworks on the edge"),
        (0, "DSF: system model and architecture"),
        (0, "Network-aware scheduling: HEFT with online profiling"),
        (0, "ε-tolerant tie-breaking under estimator noise"),
        (0, "Experimental evaluation on an 8-node tc-shaped testbed"),
        (0, "Limitations and future work"),
    ], default_size=16)
    add_speaker_notes(s,
        "Outline. Heavier on system and scheduling than on motivation; "
        "evaluation is three result figures plus an ablation.")
    return s


def slide_background(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Background: DAG-shaped workloads on edge clusters")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Edge / on-premises clusters increasingly host structured pipelines:"),
        (1, "Sensor capture → preprocessing → ML inference → action / storage"),
        (1, "Examples: IoBT mission analytics, scientific data processing, "
            "distributed network measurement"),
        (0, "These workloads are naturally DAG-shaped:"),
        (1, "Tasks have explicit dependencies and per-stage hardware affinity"),
        (1, "Inter-stage payloads range from kilobytes to hundreds of megabytes"),
        (0, "The deployment substrate is heterogeneous in two dimensions:"),
        (1, "Compute: ARM single-board computers, x86 servers, GPU nodes"),
        (1, "Network: inter-node bandwidth varies 10–20× across pairs in real "
            "deployments (mixed wired / wireless, multi-tier topologies)"),
    ], default_size=14)
    add_speaker_notes(s,
        "Set the scene. The point is that edge DAGs differ from cloud DAGs "
        "in two specific ways: hardware affinity matters, and the network "
        "between nodes is not flat.")
    return s


def slide_problem(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Problem statement")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Given:"),
        (1, "A DAG  G = (T, E)  with per-task resource demand r(t) "
            "and inter-task data volume d(t, t′)"),
        (1, "A heterogeneous cluster N with a bandwidth matrix "
            "B : N × N → ℝ⁺  and per-node capacity c(n)"),
        (1, "Per-task affinity sets  A(t) ⊆ N  (hardware-driven, e.g. GPU)"),
        (0, "Find a placement  τ : T → N  with  τ(t) ∈ A(t)  that:"),
        (1, "Minimises makespan for one-shot DAGs"),
        (1, "Minimises end-to-end latency (mean and p95) for streaming DAGs"),
        (0, "Subject to:"),
        (1, "Per-(task, node) runtime  w(t, n)  and bandwidth  B(n, n′)  "
            "are unknown a priori on real hardware"),
        (1, "The placement must be implementable inside a Kubernetes "
            "controller, with bounded memory and reconciliation latency"),
    ], default_size=13)
    add_speaker_notes(s,
        "Formal-ish problem statement. The 'unknown a priori' constraint "
        "is the part that motivates the online profiler — most HEFT papers "
        "assume w and B are given.")
    return s


def slide_existing(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Limitations of existing DAG / streaming frameworks")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Argo Workflows, Tekton, Airflow, Kubeflow Pipelines"),
        (1, "Artifact-based: every inter-task transfer goes A → object store → B"),
        (1, "Network-blind placement; no notion of inter-node bandwidth"),
        (1, "Object store (S3 / MinIO / NFS) consumes scarce edge resources"),
        (0, "Apache Kafka, Flink, Spark Streaming"),
        (1, "Heavyweight broker / coordinator clusters; not suited to "
            "small edge deployments"),
        (1, "Streaming-only; cannot express finite pipelines uniformly"),
        (0, "Ray, Dask"),
        (1, "Object-store mediated; centralised scheduling for cloud topology"),
        (1, "No first-class concept of per-task hardware affinity"),
        (0, "Common gap: the network is treated as plumbing.  Task completion "
            "and data availability are conflated, even when topology and "
            "affinity make the difference quantifiable."),
    ], default_size=13)
    add_speaker_notes(s,
        "One slide on prior work. The unifying observation across the "
        "comparison: none of these systems reason about which physical "
        "node a payload needs to land on, or about non-uniform link "
        "bandwidth.")
    return s


def slide_contributions(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Contributions")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "C1.  Network-aware HEFT scheduler with online EMA profiling for "
            "per-(task, node) runtime and per-(node, node) bandwidth on k3s."),
        (0, "C2.  ε-tolerant tie-breaking — a bounded-uncertainty extension to "
            "list scheduling that addresses estimator-noise–induced "
            "concentration of parallel work."),
        (0, "C3.  Unified Kubernetes-native abstraction for batch (ODAG) and "
            "streaming (CDAG) DAGs, with a single SDK and observability layer."),
        (0, "C4.  Brokerless P2P data plane (data-agent + ZMQ) "
            "that avoids shared object stores and message brokers."),
        (0, "C5.  Empirical evaluation on an 8-node, tc-shaped testbed "
            "using three benchmarks; 27% mean / 32% converged makespan reduction "
            "(ODAG) and 39% p95-latency reduction (CDAG) over baselines."),
    ], default_size=14)
    add_speaker_notes(s,
        "Five contributions; C1 and C2 are the research core, C3–C4 are "
        "the platform that makes them measurable, C5 is the empirical "
        "evidence.")
    return s


def slide_section(prs, title, subtitle=None, items=None):
    """
    Section divider. Uses SECTION_TITLE_AND_DESCRIPTION when items is provided,
    otherwise SECTION_HEADER.
    """
    if items:
        s = prs.slides.add_slide(prs.slide_layouts[L_SEC_DESC])
        set_title(s, title)
        if subtitle:
            set_subtitle(s, subtitle)
        body = find_placeholder(s, 2)
        if body:
            fill_body(body, [(0, x) for x in items], default_size=13)
    else:
        s = prs.slides.add_slide(prs.slide_layouts[L_SECTION])
        set_title(s, title)
    add_speaker_notes(s, f"Section: {title}.")
    return s


def slide_architecture(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_ONLY])
    set_title(s, "System architecture")

    def box(x, y, w, h, label, *, fill=CARD_BG, border=CARD_BD,
            tcolor=INK, tsize=10, bold=False, anchor=MSO_ANCHOR.MIDDLE):
        sh = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x, y, w, h)
        sh.adjustments[0] = 0.18
        sh.fill.solid(); sh.fill.fore_color.rgb = fill
        sh.line.color.rgb = border
        sh.line.width = Pt(0.5)
        tb, tf = add_textbox(s, x, y, w, h, anchor=anchor)
        add_para(tf, label, size=tsize, bold=bold, color=tcolor,
                 align=PP_ALIGN.CENTER, first=True)
        return sh

    # User column (left)
    box(Inches(0.34), Inches(1.3), Inches(2.4), Inches(0.5),
        "User  ·  dsf CLI / kubectl",
        fill=NAVY, border=NAVY, tcolor=WHITE, tsize=10, bold=True)
    box(Inches(0.34), Inches(1.95), Inches(2.4), Inches(0.5),
        "K8s API server (CRDs)",
        fill=RGBColor(0xE8, 0xEE, 0xF6), border=NAVY, tcolor=NAVY, bold=True)

    # Control plane (centre)
    cp = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                            Inches(2.95), Inches(1.20), Inches(4.2), Inches(2.85))
    cp.adjustments[0] = 0.04
    cp.fill.solid(); cp.fill.fore_color.rgb = RGBColor(0xF8, 0xFA, 0xFC)
    cp.line.color.rgb = NAVY; cp.line.width = Pt(0.75)
    tb, tf = add_textbox(s, Inches(3.05), Inches(1.27), Inches(4.0), Inches(0.3))
    add_para(tf, "dsf-system  (master)", size=10, bold=True, color=NAVY, first=True)

    box(Inches(3.05), Inches(1.6), Inches(1.95), Inches(0.6),
        "odag-controller\n(one-shot DAGs)", tsize=9, bold=True,
        fill=WHITE, border=NAVY)
    box(Inches(5.10), Inches(1.6), Inches(1.95), Inches(0.6),
        "cdag-controller\n(continuous DAGs)", tsize=9, bold=True,
        fill=WHITE, border=NAVY)
    box(Inches(3.05), Inches(2.30), Inches(4.0), Inches(0.55),
        "ui-server  ·  watch cache  ·  SQLite  ·  SSE",
        tsize=9, bold=True, fill=WHITE, border=NAVY)
    box(Inches(3.05), Inches(2.95), Inches(4.0), Inches(0.55),
        "data-agent  (DaemonSet, P2P HTTP)",
        tsize=9, bold=True, fill=WHITE, border=NAVY)

    # Worker column (right)
    box(Inches(7.30), Inches(1.20), Inches(2.4), Inches(2.85),
        "", fill=RGBColor(0xFA, 0xF4, 0xEC), border=ACCENT)
    tb, tf = add_textbox(s, Inches(7.40), Inches(1.27), Inches(2.2), Inches(0.3))
    add_para(tf, "Worker pods  (k3s)", size=10, bold=True, color=ACCENT, first=True)
    for i, y in enumerate([1.60, 2.20, 2.80]):
        box(Inches(7.40), Inches(y), Inches(2.20), Inches(0.5),
            "task pod (DSF SDK)", tsize=9, bold=True, fill=WHITE, border=ACCENT)
    tb, tf = add_textbox(s, Inches(7.40), Inches(3.40), Inches(2.20), Inches(0.4))
    add_para(tf, "ZMQ PUSH/PULL · PUB/SUB", size=8, italic=True,
             color=MUTED, align=PP_ALIGN.CENTER, first=True)

    # Notes underneath
    tb, tf = add_textbox(s, Inches(0.34), Inches(4.20),
                         Inches(9.32), Inches(0.85))
    add_para(tf, "Design choices",
             size=11, bold=True, color=NAVY, first=True, space_after=2)
    add_para(tf, "•  All ODAG pods start simultaneously; downstream blocks on recv()  "
                 "·  one ClusterIP Service per task → stable DNS",
             size=10, color=INK)
    add_para(tf, "•  CDAG reconciles every 30 s; failed pods are recreated within "
                 "constraint sets  ·  owner references → GC on CR delete",
             size=10, color=INK)

    add_speaker_notes(s,
        "Two CRDs, two controllers, one UI, one DaemonSet for P2P "
        "transfers. The shape mirrors a standard K8s controller layout, "
        "but with our scheduling and data-plane sitting inside it.")
    return s


def slide_crd_compare(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_2COL])
    set_title(s, "Two execution models in one CRD group: dsf.io/v1")
    left  = find_placeholder(s, 1)
    right = find_placeholder(s, 2)
    fill_body(left, [
        (0, "ODAG — one-shot",                     {"size": 14, "bold": True, "color": NAVY}),
        (1, "Use case: finite pipelines (ETL, mission analytics)"),
        (1, "Transport: ZMQ PUSH/PULL  +  data-agent for large files"),
        (1, "Lifecycle: all pods scheduled simultaneously, run, terminate"),
        (1, "Status: phase, makespan, completionTime, per-task node"),
        (1, "Failure handling: spec.retryPolicy.maxRetries per task"),
    ], default_size=12)
    fill_body(right, [
        (0, "CDAG — continuous",                   {"size": 14, "bold": True, "color": ACCENT}),
        (1, "Use case: streaming pipelines (sensor fusion, monitoring)"),
        (1, "Transport: ZMQ PUB/SUB  (publisher binds, subscribers connect)"),
        (1, "Lifecycle: 30-s reconcile loop recreates missing pods"),
        (1, "Status: per-task readyReplicas / desiredReplicas / node"),
        (1, "Failure handling: restartPolicy ∈ {Always, OnFailure, Never}"),
    ], default_size=12)
    add_speaker_notes(s,
        "Same CRD group, two kinds. The user's mental model is one tool "
        "for both finite and continuous workloads.")
    return s


def slide_odag_lifecycle(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "ODAG controller lifecycle")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "1.  Watch fires on CR ADDED → goroutine started for this ODAG"),
        (0, "2.  Schedule:  assignTasks() returns task → node mapping"),
        (1, "Constraint-aware random (baseline) or HEFT (network-aware)"),
        (0, "3.  Create ClusterIP Service + Pod for every task simultaneously"),
        (1, "Pods carry NodeAffinity to the assigned node"),
        (1, "DSF_PEER_<TASK> env vars injected with stable DNS endpoints"),
        (0, "4.  Run:  pods communicate via ZMQ PUSH/PULL or data-agent HTTP"),
        (0, "5.  Pod-watch loop writes status.tasks[] (phase, node, timing)"),
        (0, "6.  When all tasks Succeeded:  makespan = end − start, "
            "phase = Succeeded; profiler updates EMA estimates"),
    ], default_size=13)
    add_speaker_notes(s,
        "Six steps. The non-obvious choice is step 3 — pods are created "
        "all at once. ZMQ PUSH/PULL needs the receiver bound first; "
        "downstream tasks block on recv() until the upstream sends.")
    return s


def slide_cdag_lifecycle(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "CDAG controller: continuous reconciliation")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Reconcile every 30 s, for every CDAG known to the controller:"),
        (1, "List pods by   dsf-cdag={name}, dsf-task={task}"),
        (1, "Count alive pods (Running ∪ Pending)"),
        (1, "Read pod.Spec.NodeName for status.tasks[].node"),
        (1, "If alive < desiredReplicas:  delete Failed pods, "
            "recreate via pickNode(constraints, schedulableNodes)"),
        (1, "Update status.tasks[]  (replicas, podNames, node)"),
        (1, "phase = Running  if all tasks fully ready,  else  Degraded"),
        (0, "Properties:"),
        (1, "Stateless — relies only on live K8s state, not in-memory snapshots"),
        (1, "Constraint-respecting — pickNode never violates allowed-node sets"),
        (1, "Idempotent — repeated reconciles converge to the same state"),
    ], default_size=13)
    add_speaker_notes(s,
        "CDAG has no completion event, so reconciliation is the design. "
        "Idempotent and stateless — the controller can be killed and "
        "restarted without orphaning pods.")
    return s


def slide_sdk(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_ONLY])
    set_title(s, "Programming model: a four-line task contract")

    # Code box
    code_bg = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                                 Inches(0.34), Inches(1.30),
                                 Inches(5.30), Inches(3.40))
    code_bg.adjustments[0] = 0.04
    code_bg.fill.solid(); code_bg.fill.fore_color.rgb = RGBColor(0x1A, 0x1A, 0x1A)
    code_bg.line.fill.background()

    tb, tf = add_textbox(s, Inches(0.50), Inches(1.45),
                         Inches(5.0), Inches(3.10))
    code = [
        "from dsf_sdk import DSFTask",
        "",
        "task = DSFTask()",
        "data = task.recv(\"upstream\")",
        "result = process(data)",
        "task.send(\"downstream\", result)",
        "task.close()",
    ]
    first = True
    for line in code:
        add_para(tf, line if line else " ",
                 size=14, color=WHITE, font=CODE_FONT,
                 first=first, space_after=2)
        first = False

    # Right column: contract description
    tb, tf = add_textbox(s, Inches(5.85), Inches(1.30),
                         Inches(3.85), Inches(3.40))
    add_para(tf, "Injected by the controller",
             size=12, bold=True, color=NAVY, first=True, space_after=4)
    rows = [
        ("DSF_TRANSPORT_PATTERN", "pushpull | pubsub"),
        ("DSF_RECV_PORT",         "5555"),
        ("DSF_PEER_<TASK>",       "zmq://svc.ns.svc.cluster.local:5555"),
        ("DSF_TASK_NAME",         "capture"),
    ]
    for k, v in rows:
        p = tf.add_paragraph()
        r1 = p.add_run(); r1.text = k + "  "
        style_run(r1, size=10, bold=True, color=NAVY, font=CODE_FONT)
        r2 = p.add_run(); r2.text = v
        style_run(r2, size=10, color=INK, font=CODE_FONT)
        p.space_after = Pt(4)

    add_para(tf, "", size=4, color=INK)
    add_para(tf, "The same contract works for both ODAGs and CDAGs.  Transport "
                  "selection is hidden behind transport/router.py.",
             size=10, italic=True, color=MUTED)
    add_speaker_notes(s,
        "Four-line contract. Tasks don't know whether they're in an "
        "ODAG or CDAG — the env vars decide. Same code path for batch "
        "and streaming.")
    return s


def slide_transports(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Communication transports")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "ZMQ PUSH / PULL — one-shot DAGs"),
        (1, "Receiver binds, senders connect. LINGER = 5 s. "
            "0.2 s sleep after connect (ZMQ slow-joiner). "
            "0.3 s sleep after send (flush)."),
        (0, "ZMQ PUB / SUB — continuous DAGs"),
        (1, "Publisher binds, subscribers connect. "
            "[topic, payload] multipart frames. "
            "Subscribers SUBSCRIBE = \"\" (all topics)."),
        (0, "data-agent (HTTP) — large file transfers"),
        (1, "DaemonSet on every node. Pod writes locally, agent pushes "
            "across the network. Same-node transfer is a local copy. "
            "Decouples compute completion from data delivery."),
        (0, "All endpoints are stable ClusterIP DNS names "
            "({name}-{task}.{ns}.svc.cluster.local:5555), "
            "injected at pod creation time."),
    ], default_size=13)
    add_speaker_notes(s,
        "ZMQ for messaging because brokerless. data-agent for files "
        "because we want compute–transfer decoupling.")
    return s


def slide_ui(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Observability: web UI, history, live updates")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Stack:  Go HTTP server with K8s watch caches  +  React + React Flow + Recharts"),
        (0, "Pages:"),
        (1, "/        — ODAG list (phase, makespan, age)"),
        (1, "/odags/{ns}/{name}    — graph, tasks table, run history"),
        (1, "/cdags                — CDAG list"),
        (1, "/cdags/{ns}/{name}    — graph, replicas, node assignment"),
        (1, "/batch                — multi-ODAG HEFT Gantt"),
        (0, "Live updates via Server-Sent Events at /api/events"),
        (1, "Every K8s ADDED/MODIFIED → broadcast to subscribed clients"),
        (1, "Frontend invalidates the relevant React Query cache"),
        (0, "ODAG run history persisted in SQLite (hostPath)"),
        (1, "INSERT OR IGNORE keyed on uid + resourceVersion (idempotent)"),
        (1, "Survives ui-server restarts"),
    ], default_size=12)
    add_speaker_notes(s,
        "Read-only UI; submission goes through the CLI. SSE-driven so it "
        "feels real-time without polling. Will demo if time permits.")
    return s


def slide_random(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_ONLY])
    set_title(s, "Baseline: constraint-aware random placement")

    # Pseudocode block
    bg = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                            Inches(0.34), Inches(1.30),
                            Inches(5.20), Inches(3.30))
    bg.adjustments[0] = 0.04
    bg.fill.solid(); bg.fill.fore_color.rgb = RGBColor(0x1A, 0x1A, 0x1A)
    bg.line.fill.background()
    tb, tf = add_textbox(s, Inches(0.50), Inches(1.45),
                         Inches(4.9), Inches(3.0))
    pseudo = [
        "for each task t:",
        "    A_t  =  constraints.nodeNames",
        "          ∩ schedulableClusterNodes",
        "    if A_t == ∅:  A_t  =  schedulableClusterNodes",
        "    τ(t) ←  Uniform(A_t)",
    ]
    first = True
    for line in pseudo:
        add_para(tf, line, size=13, color=WHITE, font=CODE_FONT,
                 first=first, space_after=4)
        first = False

    # Right column: properties
    tb, tf = add_textbox(s, Inches(5.75), Inches(1.30),
                         Inches(3.95), Inches(3.30))
    add_para(tf, "Properties", size=13, bold=True, color=NAVY, first=True,
             space_after=4)
    add_para(tf, "•  Trivial, predictable, encodes hardware affinity", size=11)
    add_para(tf, "•  Scheduler logic is independent per controller (ODAG/CDAG)", size=11)
    add_para(tf, "", size=4, color=INK)
    add_para(tf, "What it does not model", size=13, bold=True, color=ACCENT,
             space_after=4)
    add_para(tf, "•  Pairwise inter-node bandwidth", size=11)
    add_para(tf, "•  Per-(task, node) runtime variability", size=11)
    add_para(tf, "•  Co-location vs spread of independent tasks", size=11)
    add_speaker_notes(s,
        "This is the floor — the scheduler we compare against. It's the "
        "right default when nothing is known about runtimes or bandwidth.")
    return s


def slide_heft(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Network-aware HEFT, adapted for k3s")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Heterogeneous Earliest-Finish-Time [Topcuoglu et al., 2002], "
            "with two adaptations for our setting:"),
        (0, "Step 1 — Upward rank (DAG priority):"),
        (1, "rank(t)  =  w(t)  +  max_{t′ ∈ succ(t)}  [ c(t, t′)  +  rank(t′) ]"),
        (1, "w(t) = average runtime;  c(t, t′) = d(t, t′) / B̄"),
        (0, "Step 2 — Greedy placement:"),
        (1, "for t in tasks ordered by descending rank:"),
        (1, "    τ(t)  =  argmin_{n ∈ A(t)}  EFT(t, n)"),
        (0, "Adaptations:"),
        (1, "Bandwidth-weighted edge cost uses learned B(n, n′), not a constant"),
        (1, "Resource-aware nodeAvail (HEFT v2): models that k3s schedules "
            "independent pods on the same node in parallel "
            "when CPU and memory permit"),
    ], default_size=12)
    add_speaker_notes(s,
        "HEFT itself isn't novel. The contribution is (i) running it "
        "online with a profiler and (ii) modelling parallel pod execution "
        "on the same node — classical HEFT assumes sequential.")
    return s


def slide_profiler(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Online profiler: EMA estimates of runtime and bandwidth")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "EMA update (per (task, node) and per (node, node) pair):"),
        (1, "μ_{i+1}  =  α · sample  +  (1 − α) · μ_i        with  α = 0.7"),
        (0, "Resolver chain at scheduling time:"),
        (1, "1.  profiler estimate, if  #samples ≥ minSamples"),
        (1, "2.  spec hint  (runtime, dataSize from YAML)"),
        (1, "3.  template default  (scheduler-level fallback)"),
        (1, "4.  hardcoded floor  (never NaN)"),
        (0, "Settings:  warmupRuns = 0,  minSamples = 2,  maxSamples = 50"),
        (0, "Persistence:  EMA state stored in SQLite alongside ODAG run "
            "history; survives controller restart."),
        (0, "Implication:  HEFT inputs are learned online, with no "
            "operator-supplied per-task profile required."),
    ], default_size=13)
    add_speaker_notes(s,
        "Cheap, robust, defensible. The resolver chain is the part "
        "reviewers may push on — we never let HEFT see NaN, even on run 0.")
    return s


def slide_eps(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "ε-tolerant tie-breaking under estimator noise")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Pathology — observed in the IoBT 4-sensor benchmark:"),
        (1, "All four parallel infer-i tasks land on a single compute node, "
            "across consecutive runs, despite three candidate nodes with "
            "near-identical EMA-learned runtimes."),
        (0, "Cause:  EMA estimates are point estimates with hidden uncertainty.  "
            "Strict EFT comparison ( < ) makes the tiebreaker the iteration order."),
        (0, "Fix — bounded tolerance:"),
        (1, "minEFT  =  min_{n}  EFT(t, n)"),
        (1, "candidates  =  { n  :  EFT(t, n) ≤ minEFT + ε }"),
        (1, "τ(t)  =  arg min_{n ∈ candidates}  load(n)"),
        (0, "Properties:"),
        (1, "ε = 0  is already strictly better than classical HEFT "
            "(exact ties broken by load instead of iteration order)"),
        (1, "ε > 0  absorbs profiler noise; mean makespan unchanged, "
            "p95 tightens; placement entropy increases"),
        (1, "Generalises naturally to variance-aware EFT:  EFT  +  k · σ"),
    ], default_size=12)
    add_speaker_notes(s,
        "This is the contribution that emerged from a debugging session "
        "and turned out to be the cleanest research point. Bounded knob, "
        "measurable Pareto front, principled framing.")
    return s


def slide_eval_setup(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_ONLY])
    set_title(s, "Experimental setup: 8-node tc-shaped cluster")

    # Left column: cluster description (text card)
    add_card(s, Inches(0.34), Inches(1.30), Inches(4.40), Inches(3.55))
    tb, tf = add_textbox(s, Inches(0.50), Inches(1.40),
                         Inches(4.10), Inches(3.40))
    add_para(tf, "Cluster", size=12, bold=True, color=NAVY, first=True, space_after=2)
    add_para(tf, "•  k3s, dsf-system namespace", size=11)
    add_para(tf, "•  8 schedulable workers (anrg-1, 3..9)", size=11)
    add_para(tf, "•  Master anrg-2 (NoSchedule)", size=11)
    add_para(tf, "•  Local registry on master", size=11)
    add_para(tf, "", size=4, color=INK)
    add_para(tf, "tc bandwidth matrix (asymmetric, 20×)",
             size=12, bold=True, color=NAVY, space_after=2)
    add_para(tf, "•  F = 1 Gbps  (same-tier)", size=11)
    add_para(tf, "•  M = 100 Mbps  (cross-tier)", size=11)
    add_para(tf, "•  S = 50 Mbps  (engineered bottlenecks)", size=11)
    add_para(tf, "", size=4, color=INK)
    add_para(tf, "Benchmarks  (3 ODAGs)",
             size=12, bold=True, color=NAVY, space_after=2)
    add_para(tf, "•  iobt — 14 tasks, 5 layers, 80–150 MB", size=11)
    add_para(tf, "•  hetero-compute — 5 tasks, runtime hints", size=11)
    add_para(tf, "•  wide-pipeline-flex — fan-out × 2, fan-in × 2", size=11)

    # Right: bandwidth matrix figure
    add_image_or_placeholder(s, FIG_BANDWIDTH_MATRIX,
                             Inches(4.95), Inches(1.30),
                             Inches(4.75), Inches(3.55),
                             "tc bandwidth matrix (8×8 heatmap)")
    add_speaker_notes(s,
        "20× asymmetry sounds extreme but it's well within real edge "
        "(WAN + LAN, or 5G + wired). The bottleneck pairs are engineered "
        "so HEFT has somewhere to be smart.")
    return s


def slide_result(prs, title, fig, caption_lines, fig_label):
    """Generic result slide: TITLE_ONLY + figure left + caption right."""
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_ONLY])
    set_title(s, title)
    add_image_or_placeholder(s, fig,
                             Inches(0.34), Inches(1.30),
                             Inches(6.30), Inches(3.55),
                             fig_label)
    add_card(s, Inches(6.80), Inches(1.30), Inches(2.90), Inches(3.55))
    tb, tf = add_textbox(s, Inches(6.95), Inches(1.40),
                         Inches(2.65), Inches(3.40))
    first = True
    for line in caption_lines:
        if isinstance(line, tuple):
            kind, text = line
        else:
            kind, text = "body", line
        if kind == "head":
            add_para(tf, text, size=11, bold=True, color=NAVY,
                     first=first, space_after=3)
        elif kind == "accent":
            add_para(tf, text, size=11, bold=True, color=ACCENT,
                     first=first, space_after=3)
        elif kind == "muted":
            add_para(tf, text, size=10, italic=True, color=MUTED,
                     first=first, space_after=2)
        else:
            add_para(tf, text, size=10, color=INK,
                     first=first, space_after=2)
        first = False
    return s


def slide_exp1a(prs):
    s = slide_result(
        prs,
        "Experiment 1A: makespan, random vs HEFT vs ε-HEFT",
        FIG_MAKESPAN_DIST,
        [
            ("head",   "Setup"),
            "iobt + hetero-compute + wide-pipeline-flex",
            "20 runs per (benchmark × scheduler)",
            "Warm makespans (post-convergence)",
            ("head",   "Headline (iobt)"),
            "random:  35.5 s   (σ 5.5 s)",
            "HEFT:    25.8 s   (σ 4.7 s)",
            "HEFT*:   24.0 s   (σ 0.8 s)  — converged",
            ("accent", "−27 % overall, −32 % converged"),
            ("muted",  "* runs ≥ 4 (after EMA convergence)"),
        ],
        "makespan distribution boxplots — 3 ODAGs × 3 configs")
    add_speaker_notes(s,
        "Headline result. Two things to call out: the mean drop and the "
        "variance collapse (5.5 → 0.8 s). Real-time edge cares about both.")
    return s


def slide_convergence(prs):
    s = slide_result(
        prs,
        "Profiler convergence: cold-start to optimal in 3–4 runs",
        FIG_MAKESPAN_CONV,
        [
            ("head",   "Reading the curve"),
            "Run 1: HEFT > random (cold profiler)",
            "Runs 2–3: EMA accumulates samples",
            "Runs ≥ 4: HEFT settles ~32 % below random",
            ("head",   "Caveat"),
            ("muted",  "First-run cost is real; not hidden in our reporting"),
        ],
        "makespan vs run index, three schedulers")
    add_speaker_notes(s,
        "Don't gloss over the cold-start cost — call it out. Honest "
        "story: 3–4 runs to converge, run 1 sometimes worse than random, "
        "stable thereafter.")
    return s


def slide_eps_ablation(prs):
    s = slide_result(
        prs,
        "ε-HEFT ablation: placement spreads, mean unchanged",
        FIG_INFER_PLACEMENT,
        [
            ("head",   "iobt infer-i placement share"),
            "ε = 0 (strict):  one node dominates",
            "ε = 0.5:   two nodes share the work",
            "ε = 1.0:   ~uniform across three nodes",
            "ε = 2.0:   uniform (saturates)",
            ("head",   "Effect on makespan"),
            "Mean ≈ unchanged across ε",
            ("accent", "p95 tightens as ε grows"),
            ("muted",  "Spread vs efficiency Pareto front"),
        ],
        "iobt infer-i placement share vs ε")
    add_speaker_notes(s,
        "Justifies ε as a real contribution and not a heuristic. Bounded "
        "knob, measurable effect, honest saturating curve.")
    return s


def slide_prediction(prs):
    s = slide_result(
        prs,
        "Calibration: predicted makespan vs measured wall-clock",
        FIG_PRED_SCATTER,
        [
            ("head",   "Reading the scatter"),
            "Cluster around y = x → calibrated",
            "Slight under-prediction in early warm runs",
            ("head",   "Sources of error not modelled"),
            "Image-pull jitter",
            "Same-node memory contention",
            "Pod-startup variance",
        ],
        "predicted vs actual makespan; y = x reference")
    add_speaker_notes(s,
        "Use this slide to discuss model fidelity. The right framing: "
        "predicted is good enough to drive placement, not good enough "
        "to be quoted as wall-clock SLA.")
    return s


def slide_cdag_eval(prs):
    s = slide_result(
        prs,
        "Experiment 1B: CDAG random vs locality (latency)",
        FIG_CDAG_LATENCY,
        [
            ("head",   "Camera fusion pipeline"),
            "9 tasks, 5 MB frames at 1 Hz",
            "5 instances × 120 s each",
            ("head",   "Headline"),
            "Mean latency:  590 → 462 ms  (−22 %)",
            ("accent", "p95 latency:  1238 → 749 ms  (−39 %)"),
            "Throughput unchanged (camera-bound)",
            ("muted",  "Tail dominates real-time edge SLAs"),
        ],
        "CDAG latency / throughput — random vs locality")
    add_speaker_notes(s,
        "Tail latency is the headline for streaming. 39 % p95 reduction "
        "matters more than the 22 % mean reduction for any real-time "
        "pipeline.")
    return s


def slide_scalability(prs):
    s = slide_result(
        prs,
        "Experiment 2: P2P vs centralised data plane",
        FIG_SCALABILITY,
        [
            ("head",   "Topology"),
            "source → N workers → sink, N ∈ {2, 4, 6, 8}",
            ("head",   "ODAG: P2P vs NFS"),
            "P2P data-agent: parallel HTTP push",
            "NFS: all I/O funnels through one pod",
            ("muted",  "Expected: NFS makespan grows with N"),
            ("head",   "CDAG: ZMQ vs MQTT"),
            "ZMQ: direct PUB/SUB",
            "MQTT: source → broker → subscribers",
            ("muted",  "Expected: ZMQ throughput linear; MQTT plateaus"),
        ],
        "scalability: P2P vs centralised, ODAG and CDAG")
    add_speaker_notes(s,
        "Mostly a structural argument — at edge sizes the broker overhead "
        "is large because the cluster is small.")
    return s


def slide_lessons(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Lessons learned during implementation")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Pod parallelism on a single node breaks classical HEFT"),
        (1, "k3s schedules independent pods in parallel when resources permit; "
            "HEFT v2 models nodeAvail as resource intervals, not a scalar."),
        (0, "EMA-learned runtimes converge — and tie"),
        (1, "Per-task estimates on equivalent compute nodes converge to "
            "near-identical values; strict EFT then concentrates load."),
        (0, "ZMQ is timing-sensitive even for small messages"),
        (1, "LINGER and slow-joiner sleeps are not optional in "
            "multi-pod K8s deployments; revisited every ZMQ pitfall."),
        (0, "imagePullPolicy interacts with first-run measurements"),
        (1, "imagePullPolicy: Always is required for correctness with :latest "
            "tags; first-run overhead must be reported separately."),
        (0, "K8s API quirk:  CRD type number is stored as int64"),
        (1, "Strict float64 assertion fails silently; nestedFloat() helper "
            "handles the three observed encodings."),
    ], default_size=12)
    add_speaker_notes(s,
        "Five honest 'what surprised us' bullets. Lab audiences value "
        "these more than polished results.")
    return s


def slide_limitations(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Limitations")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Single-cluster, single-region testbed"),
        (1, "All evaluation on a LAN with engineered tc shaping; "
            "WAN-scale or multi-cluster scenarios not validated."),
        (0, "Profiler is keyed on (task, node), not input size"),
        (1, "Variable-input pipelines may need finer-grained estimates."),
        (0, "ε is a fixed scalar"),
        (1, "Variance-aware EFT (EFT + k·σ) is a natural extension; "
            "not yet implemented."),
        (0, "No fault-injection campaign"),
        (1, "data-agent retries (5×, 500 ms backoff) are unmeasured under "
            "partial-network partitions."),
        (0, "Broker-comparison numbers (Experiment 2) collected but not "
            "yet analysed at full breadth."),
    ], default_size=12)
    add_speaker_notes(s,
        "Lay these out before the audience does. A clean limitations "
        "slide is worth two reviewer rounds.")
    return s


def slide_future(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Future work")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "Variance-aware HEFT"),
        (1, "Move from ε-tolerance to a proper risk-adjusted EFT, "
            "EFT + k·σ, with σ from the EMA second moment."),
        (0, "Cross-CDAG flow tracker"),
        (1, "Locality scheduler aware of bandwidth already in use by "
            "other concurrent CDAGs."),
        (0, "DataReady triggering"),
        (1, "Fire downstream tasks when data lands on their node, not "
            "when the upstream pod exits — closes the compute–transfer "
            "decoupling story."),
        (0, "Per-input-size profiler keys"),
        (1, "Per-(task, node, input-bucket) estimates for variable-input "
            "pipelines."),
        (0, "Paper:  targeting a systems venue (SEC / SoCC / EuroSys 2026)"),
    ], default_size=13)
    add_speaker_notes(s,
        "Open with variance-aware HEFT — it's the most natural follow-up "
        "to ε. End on the paper target so the room knows what the next "
        "deliverable is.")
    return s


def slide_summary(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Summary")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "DSF is a network-aware DAG scheduling framework for k3s, "
            "supporting both one-shot and continuous pipelines under one CRD group."),
        (0, "Network-aware HEFT with online EMA profiling reduces makespan "
            "by 27 % (mean) / 32 % (converged) on an 8-node testbed; "
            "variance drops from 5.5 s to 0.8 s."),
        (0, "ε-tolerant tie-breaking spreads parallel work without losing "
            "EFT optimality; p95 makespan tightens as ε grows."),
        (0, "Brokerless P2P data plane (ZMQ + data-agent) avoids the "
            "shared-store and broker overhead that dominates small clusters."),
        (0, "Code: github.com/anrg/dsf"),
    ], default_size=14)
    add_speaker_notes(s,
        "Close on these four bullets. One sentence each. Then questions.")
    return s


def slide_acks(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_BODY])
    set_title(s, "Acknowledgements")
    body = find_placeholder(s, 1)
    fill_body(body, [
        (0, "ANRG group members for discussions and feedback throughout the "
            "implementation, evaluation, and ε-HEFT design."),
        (0, "Cluster infrastructure provided by the Autonomous Networks "
            "Research Group at USC Viterbi."),
        (0, "Foundational work: Topcuoglu et al. (2002) on HEFT, "
            "and the broader literature on workflow scheduling, "
            "stream processing, and Kubernetes-native orchestration."),
        (0, "This work was supported in part by [grant/agency], "
            "[award number].  Any opinions and conclusions are those "
            "of the author and do not reflect those of the funding agencies."),
    ], default_size=12)
    add_speaker_notes(s,
        "Standard ANRG acknowledgements slide. Fill in the actual grant "
        "before presenting.")
    return s


def slide_thanks(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_SECTION])
    set_title(s, "Thank you.   Questions?")
    add_speaker_notes(s, "Open the floor.")
    return s


def slide_backup_perf(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_ONLY])
    set_title(s, "Backup: numerical results")
    rows = [
        ("Bench",      "Config",  "Mean (s)",  "σ (s)",  "p95 (s)",  "Δ vs random"),
        ("iobt",       "random",  "35.5",      "5.5",    "44.0",     "—"),
        ("iobt",       "HEFT",    "25.8",      "4.7",    "32.5",     "−27 %"),
        ("iobt",       "HEFT*",   "24.0",      "0.8",    "25.6",     "−32 %"),
        ("iobt",       "ε-HEFT",  "24.1",      "0.7",    "24.9",     "−32 %"),
        ("hetero-c.",  "random",  "—",         "—",      "—",        "—"),
        ("hetero-c.",  "HEFT",    "—",         "—",      "—",        "—"),
        ("wide-flex",  "random",  "—",         "—",      "—",        "—"),
        ("wide-flex",  "HEFT",    "—",         "—",      "—",        "—"),
    ]
    cols = [Inches(1.5), Inches(1.4), Inches(1.3), Inches(1.1), Inches(1.3), Inches(1.5)]
    x_starts = [Inches(0.5)]
    for c in cols[:-1]:
        x_starts.append(x_starts[-1] + c)
    y0 = Inches(1.4); rh = Inches(0.34)
    for ri, row in enumerate(rows):
        is_header = (ri == 0)
        for ci, val in enumerate(row):
            sh = s.shapes.add_shape(MSO_SHAPE.RECTANGLE,
                                    x_starts[ci], y0 + ri * rh,
                                    cols[ci], rh)
            sh.line.color.rgb = CARD_BD; sh.line.width = Pt(0.4)
            sh.fill.solid()
            sh.fill.fore_color.rgb = (NAVY if is_header else
                                      (WHITE if ri % 2 else CARD_BG))
            tb, tf = add_textbox(s, x_starts[ci], y0 + ri * rh,
                                 cols[ci], rh, anchor=MSO_ANCHOR.MIDDLE)
            add_para(tf, val, size=10, bold=is_header,
                     color=WHITE if is_header else INK,
                     align=PP_ALIGN.CENTER, first=True)
    tb, tf = add_textbox(s, Inches(0.5), Inches(4.65),
                         Inches(9.0), Inches(0.4))
    add_para(tf, "* HEFT, runs 4+ (post-convergence).  "
                  "Em-dashes denote values not yet committed at this draft.",
             size=10, italic=True, color=MUTED, first=True)
    add_speaker_notes(s, "Backup table for Q&A.")
    return s


def slide_backup_dag(prs):
    s = prs.slides.add_slide(prs.slide_layouts[L_TITLE_ONLY])
    set_title(s, "Backup: IoBT DAG topology (14 tasks, 5 layers)")
    add_image_or_placeholder(s, None,
                             Inches(0.34), Inches(1.30),
                             Inches(9.32), Inches(3.55),
                             "IoBT DAG: capture → preprocess → infer → fuse → report")
    add_speaker_notes(s, "Backup. Pull up if asked about the benchmark structure.")
    return s


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
def build():
    prs = Presentation(str(TEMPLATE))
    clear_slides(prs)

    # Front matter — academic register
    slide_title(prs)
    slide_outline(prs)
    slide_background(prs)
    slide_problem(prs)
    slide_existing(prs)
    slide_contributions(prs)

    # Part I — System
    slide_section(prs, "Part I — System architecture",
                  subtitle="What DSF is",
                  items=["CRDs and controllers",
                         "Programming model",
                         "Communication transports",
                         "Observability"])
    slide_architecture(prs)
    slide_crd_compare(prs)
    slide_odag_lifecycle(prs)
    slide_cdag_lifecycle(prs)
    slide_sdk(prs)
    slide_transports(prs)
    slide_ui(prs)

    # Part II — Scheduling
    slide_section(prs, "Part II — Scheduling",
                  subtitle="From random placement to ε-HEFT",
                  items=["Constraint-aware random baseline",
                         "Network-aware HEFT",
                         "Online EMA profiler",
                         "ε-tolerant tie-breaking"])
    slide_random(prs)
    slide_heft(prs)
    slide_profiler(prs)
    slide_eps(prs)

    # Part III — Evaluation
    slide_section(prs, "Part III — Evaluation",
                  subtitle="What we measured",
                  items=["8-node tc-shaped testbed",
                         "Three ODAG benchmarks",
                         "ε-HEFT ablation",
                         "Calibration and scalability"])
    slide_eval_setup(prs)
    slide_exp1a(prs)
    slide_convergence(prs)
    slide_eps_ablation(prs)
    slide_prediction(prs)
    slide_cdag_eval(prs)
    slide_scalability(prs)

    # Part IV — Discussion
    slide_section(prs, "Part IV — Discussion",
                  subtitle="Lessons, limitations, and what's next",
                  items=["Implementation lessons",
                         "Limitations",
                         "Future directions"])
    slide_lessons(prs)
    slide_limitations(prs)
    slide_future(prs)

    # Wrap
    slide_summary(prs)
    slide_acks(prs)
    slide_thanks(prs)

    # Backup
    slide_backup_dag(prs)
    slide_backup_perf(prs)

    prs.save(OUT_PATH)
    print(f"Wrote {OUT_PATH}  ({len(prs.slides)} slides, "
          f"{prs.slide_width/914400:.2f}\" x {prs.slide_height/914400:.2f}\")")


if __name__ == "__main__":
    build()
