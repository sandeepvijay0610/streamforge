import math
from datetime import datetime, timezone
from html import escape

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

import queries

REFRESH_MS = 10000

INK = "#17212B"
MUTED = "#66727F"
ACCENT = "#2B5C8A"
OK = "#3C7D5E"
FAIL = "#B4432F"
WARN = "#B7791F"
NEUTRAL = "#8795A5"

STATUS_COLORS = {"SUCCESS": OK, "FAILED": FAIL, "PENDING": NEUTRAL, "REFUNDED": WARN}
SEVERITY_COLORS = {
    "LOW": "#B7791F",
    "MEDIUM": "#C2571A",
    "HIGH": "#B4432F",
    "CRITICAL": "#6B3FA0",
}

st.set_page_config(
    page_title="StreamForge", layout="wide", initial_sidebar_state="collapsed"
)
st_autorefresh(interval=REFRESH_MS, key="streamforge_refresh")

# One stylesheet, no blank lines (Streamlit's markdown fallback would end the
# HTML block at the first blank line).
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Public+Sans:wght@400;500;600;700&display=swap');
:root { --ink: #17212B; --muted: #66727F; --rule: #DCE1E7; --bg: #F7F8FA; --gap: clamp(.9rem, 1.4vw, 2.2rem); }
html { font-size: clamp(14px, .42vw + 8px, 28px); }
html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"] { background: var(--bg); }
[data-testid="stHeader"], [data-testid="stToolbar"], [data-testid="stDecoration"], #MainMenu, footer { display: none !important; }
.block-container, [data-testid="stMainBlockContainer"], [data-testid="stAppViewBlockContainer"] { padding: 0 !important; max-width: 100% !important; }
[data-testid="stVerticalBlock"] { gap: 0 !important; }
.element-container:has(iframe), [data-testid="stElementContainer"]:has(iframe) { position: absolute; width: 0; height: 0; overflow: hidden; margin: 0; }
.sf { box-sizing: border-box; height: 100vh; height: 100dvh; min-height: 42rem; padding: var(--gap); display: grid; gap: var(--gap); color: var(--ink); font-family: 'Public Sans', system-ui, -apple-system, 'Segoe UI', sans-serif; grid-template-columns: minmax(0, 5fr) minmax(0, 3fr) minmax(0, 4fr); grid-template-rows: auto auto minmax(0, 1.1fr) minmax(0, 1fr); grid-template-areas: "head head head" "kpi kpi kpi" "vol mer ale" "fee brk ale"; }
.sf *, .sf *::before, .sf *::after { box-sizing: border-box; }
.sf-head { grid-area: head; display: flex; justify-content: space-between; align-items: baseline; flex-wrap: wrap; gap: .4rem 1.5rem; }
.sf-title { font-size: 1.7rem; font-weight: 700; letter-spacing: -.015em; line-height: 1.1; margin: 0; padding: 0; color: var(--ink); }
.sf-status { font-size: .85rem; color: var(--muted); font-variant-numeric: tabular-nums; }
.sf-kpis { grid-area: kpi; display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); border-top: 2px solid var(--ink); border-bottom: 1px solid var(--rule); }
.sf-kpi { min-width: 0; padding: .8rem 1rem .9rem 0; }
.sf-kpi + .sf-kpi { border-left: 1px solid var(--rule); padding-left: 1.1rem; }
.sf-kpi-label { font-size: .85rem; color: var(--muted); }
.sf-kpi-value { font-size: 2.1rem; font-weight: 600; letter-spacing: -.02em; line-height: 1.2; font-variant-numeric: tabular-nums; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.sf-kpi-sub { font-size: .8rem; color: var(--muted); min-height: 1.2em; }
.sf-panel { display: flex; flex-direction: column; min-width: 0; min-height: 0; border-top: 1px solid var(--rule); padding-top: .7rem; }
.sf-ph { flex: none; display: flex; align-items: baseline; justify-content: space-between; gap: 1rem; margin-bottom: .7rem; }
.sf-ph h3 { font-size: 1.02rem; font-weight: 600; line-height: 1.2; margin: 0; padding: 0; color: var(--ink); }
.sf-note { font-size: .82rem; color: var(--muted); }
.sf-body { position: relative; flex: 1 1 0; min-height: 0; }
.sf-fill { position: absolute; inset: 0; overflow: auto; }
.sf-vol { grid-area: vol; }
.sf-mer { grid-area: mer; }
.sf-ale { grid-area: ale; }
.sf-fee { grid-area: fee; }
.sf-brk { grid-area: brk; display: grid; grid-template-rows: minmax(0, 1fr) minmax(0, 1fr); gap: var(--gap); min-width: 0; min-height: 0; }
.sf-keys { display: flex; gap: 1rem; font-size: .82rem; color: var(--muted); }
.sf-key { display: inline-flex; align-items: center; gap: .4rem; }
.sf-key i { display: inline-block; width: .9rem; height: .2rem; border-radius: .1rem; }
.sf-plot { position: absolute; inset: .5rem 0 0 0; display: grid; grid-template-columns: auto minmax(0, 1fr); grid-template-rows: minmax(0, 1fr) 1.6rem; column-gap: .7rem; }
.sf-yax { position: relative; min-width: 2.4rem; }
.sf-yax span { position: absolute; right: 0; transform: translateY(-50%); font-size: .78rem; line-height: 1; color: var(--muted); font-variant-numeric: tabular-nums; }
.sf-cv { position: relative; }
.sf-cv svg { position: absolute; inset: 0; width: 100%; height: 100%; overflow: visible; }
.sf-xax { position: relative; }
.sf-xax span { position: absolute; top: .5rem; transform: translateX(-50%); font-size: .78rem; line-height: 1; color: var(--muted); white-space: nowrap; font-variant-numeric: tabular-nums; }
.sf-gl { stroke: #E6EAEF; stroke-width: 1px; vector-effect: non-scaling-stroke; }
.sf-gl.base { stroke: #C9D0D8; }
.sf-ln { fill: none; stroke-width: 2px; stroke-linejoin: round; stroke-linecap: round; vector-effect: non-scaling-stroke; }
.sf-pt { position: absolute; width: .6rem; height: .6rem; border-radius: 50%; transform: translate(-50%, -50%); border: 2px solid var(--bg); }
.sf-bars { position: absolute; inset: 0; display: flex; flex-direction: column; overflow: auto; }
.sf-bar { flex: 1 1 0; min-height: 1.7rem; max-height: 4.2rem; display: grid; grid-template-columns: minmax(4rem, 30%) minmax(0, 1fr); align-items: center; column-gap: .8rem; font-size: .9rem; }
.sf-bar-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.sf-bar-track { display: flex; align-items: center; gap: .6rem; min-width: 0; }
.sf-bar-fill { flex: none; height: 1rem; min-width: .15rem; border-radius: 0 .15rem .15rem 0; }
.sf-bar-val { font-size: .85rem; color: var(--muted); white-space: nowrap; font-variant-numeric: tabular-nums; }
.sf-table { width: 100%; border-collapse: collapse; font-size: .88rem; font-variant-numeric: tabular-nums; }
.sf-table th { position: sticky; top: 0; background: var(--bg); text-align: left; font-weight: 500; font-size: .8rem; color: var(--muted); padding: .3rem .8rem .5rem 0; border-bottom: 1px solid var(--rule); }
.sf-table td { padding: .55rem .8rem .55rem 0; border-bottom: 1px solid #E9EDF1; white-space: nowrap; }
.sf-table .num { text-align: right; }
.sf-table th:last-child, .sf-table td:last-child { padding-right: 0; }
.sf-st { display: inline-block; width: .55rem; height: .55rem; border-radius: 50%; margin-right: .5rem; }
.sf-alert { border-left: .2rem solid var(--sev); padding: .05rem 0 .05rem .9rem; margin: 0 .8rem 1.1rem 0; }
.sf-alert-top { display: flex; align-items: baseline; gap: .7rem; font-size: .9rem; }
.sf-sev { font-weight: 600; color: var(--sev); }
.sf-rule { font-weight: 500; }
.sf-when { margin-left: auto; color: var(--muted); font-variant-numeric: tabular-nums; }
.sf-alert-desc { font-size: .88rem; line-height: 1.4; margin-top: .15rem; }
.sf-alert-meta { font-size: .8rem; color: var(--muted); margin-top: .1rem; }
.sf-empty { color: var(--muted); font-size: .92rem; line-height: 1.5; max-width: 34rem; padding-top: .3rem; }
.sf-foot { grid-area: foot; font-size: .8rem; color: var(--muted); }
@media (max-width: 1100px), (max-aspect-ratio: 1/1) {
  .sf { height: auto; min-height: 0; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); grid-template-rows: none; grid-template-areas: "head head" "kpi kpi" "vol vol" "mer brk" "fee fee" "ale ale"; }
  .sf-panel { min-height: 17rem; }
  .sf-brk { min-height: 0; }
  .sf-brk .sf-panel { min-height: 12rem; }
  .sf-kpis { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .sf-kpi:nth-child(3n+1) { border-left: 0; padding-left: 0; }
  .sf-fee .sf-body, .sf-ale .sf-body { min-height: 20rem; }
}
@media (max-width: 640px) {
  .sf { grid-template-columns: minmax(0, 1fr); grid-template-areas: "head" "kpi" "vol" "mer" "brk" "fee" "ale"; }
  .sf-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .sf-kpi:nth-child(3n+1) { border-left: 1px solid var(--rule); padding-left: 1.1rem; }
  .sf-kpi:nth-child(odd) { border-left: 0; padding-left: 0; }
}
"""


def _e(value) -> str:
    """Escape for HTML ('$' too, so the markdown fallback can't read it as LaTeX)."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    return escape(str(value)).replace("$", "&#36;")


def _format_minor(minor, currency: str = "INR") -> str:
    if minor is None or pd.isna(minor):
        return "—"
    symbol = {"INR": "₹", "USD": "$", "EUR": "€", "GBP": "£", "SGD": "S$"}.get(currency, "")
    return f"{symbol}{int(minor) / 100:,.2f}"


def _count(value) -> str:
    return "—" if value is None or pd.isna(value) else f"{int(value):,}"


def _naive_utc(series: pd.Series) -> pd.Series:
    """UTC wall-clock time, so every timestamp on the page matches the header."""
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def _render(markup: str) -> None:
    if hasattr(st, "html"):
        st.html(markup)
    else:
        st.markdown(markup, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Building blocks (all return HTML strings; no newlines, no blank lines)
# ---------------------------------------------------------------------------
def _note(text: str) -> str:
    return f'<span class="sf-note">{_e(text)}</span>'


def _panel(title: str, body: str, note: str = "", cls: str = "") -> str:
    return (
        f'<section class="sf-panel {cls}">'
        f'<div class="sf-ph"><h3>{_e(title)}</h3>{note}</div>'
        f'<div class="sf-body">{body}</div>'
        "</section>"
    )


def _empty(message: str) -> str:
    return f'<div class="sf-fill"><div class="sf-empty">{_e(message)}</div></div>'


def _kpi(label: str, value: str, sub: str = "", sub_color: str = "") -> str:
    style = f' style="color:{sub_color}"' if sub_color else ""
    return (
        '<div class="sf-kpi">'
        f'<div class="sf-kpi-label">{_e(label)}</div>'
        f'<div class="sf-kpi-value">{_e(value)}</div>'
        f'<div class="sf-kpi-sub"{style}>{_e(sub)}</div>'
        "</div>"
    )


def _bars(df, category, value, *, color=ACCENT, color_map=None, fmt=None, label=None):
    d = df.copy()
    d[value] = pd.to_numeric(d[value])
    top = float(d[value].max()) or 1.0
    fmt = fmt or (lambda v: f"{v:,.0f}")
    label = label or (lambda s: s)
    rows = []
    for _, r in d.iterrows():
        v = float(r[value])
        name = str(r[category])
        c = (color_map or {}).get(name.upper(), color)
        width = max(v / top * 78, 0)
        rows.append(
            '<div class="sf-bar">'
            f'<div class="sf-bar-name" title="{_e(name)}">{_e(label(name))}</div>'
            '<div class="sf-bar-track">'
            f'<div class="sf-bar-fill" style="width:{width:.1f}%;background:{c}"></div>'
            f'<span class="sf-bar-val">{_e(fmt(v))}</span>'
            "</div></div>"
        )
    return '<div class="sf-bars">' + "".join(rows) + "</div>"


def _axis(peak: float):
    """Return (step, intervals) so the y axis ends on a round number."""
    peak = max(peak, 1)
    if peak <= 4:
        return 1, int(math.ceil(peak))
    raw = peak / 4
    mag = 10 ** math.floor(math.log10(raw))
    step = mag
    for f in (1, 2, 5, 10):
        step = f * mag
        if raw <= step:
            break
    return step, 4 if peak <= step * 4 else 5


def _volume_chart(vol: pd.DataFrame) -> str:
    d = vol.copy()
    d["minute"] = _naive_utc(d["minute"])
    d = d.sort_values("minute")
    times = d["minute"].tolist()
    total = pd.to_numeric(d["transaction_count"]).tolist()
    failed = pd.to_numeric(d["failed_count"]).tolist()

    step, intervals = _axis(max(total))
    ymax = step * intervals
    t0, t1 = times[0], times[-1]
    span = (t1 - t0).total_seconds()

    def x_of(ts) -> float:
        return 50.0 if span == 0 else 1.5 + 97.0 * (ts - t0).total_seconds() / span

    def y_of(v) -> float:
        return 100.0 - (v / ymax) * 100.0

    grid = "".join(
        f'<line class="sf-gl{" base" if i == 0 else ""}" x1="0" x2="100" '
        f'y1="{100 - i * 100 / intervals:.2f}" y2="{100 - i * 100 / intervals:.2f}"/>'
        for i in range(intervals + 1)
    )
    all_pts = [(x_of(t), y_of(v)) for t, v in zip(times, total)]
    fail_pts = [(x_of(t), y_of(v)) for t, v in zip(times, failed)]

    def pts(points):
        return " ".join(f"{x:.2f},{y:.2f}" for x, y in points)

    shapes = ""
    if len(all_pts) > 1:
        area = [(all_pts[0][0], 100.0)] + all_pts + [(all_pts[-1][0], 100.0)]
        shapes += f'<polygon points="{pts(area)}" fill="{ACCENT}" fill-opacity=".1"/>'
        shapes += f'<polyline class="sf-ln" points="{pts(all_pts)}" stroke="{ACCENT}"/>'
        if max(failed) > 0:
            shapes += f'<polyline class="sf-ln" points="{pts(fail_pts)}" stroke="{FAIL}"/>'

    dots = ""
    if len(times) <= 24:
        for (x, y), f in zip(all_pts, failed):
            dots += f'<span class="sf-pt" style="left:{x:.2f}%;top:{y:.2f}%;background:{ACCENT}"></span>'
        for (x, y), f in zip(fail_pts, failed):
            if f > 0:
                dots += f'<span class="sf-pt" style="left:{x:.2f}%;top:{y:.2f}%;background:{FAIL}"></span>'

    yax = "".join(
        f'<span style="top:{100 - i * 100 / intervals:.2f}%">{_e(f"{step * i:,.0f}")}</span>'
        for i in range(intervals + 1)
    )
    if span == 0:
        xax = f'<span style="left:50%">{t0.strftime("%H:%M")}</span>'
    else:
        xax = "".join(
            f'<span style="left:{1.5 + 97.0 * i / 4:.2f}%">'
            f'{(t0 + (t1 - t0) * i / 4).strftime("%H:%M")}</span>'
            for i in range(5)
        )

    return (
        '<div class="sf-plot">'
        f'<div class="sf-yax">{yax}</div>'
        f'<div class="sf-cv"><svg viewBox="0 0 100 100" preserveAspectRatio="none">{grid}{shapes}</svg>{dots}</div>'
        "<div></div>"
        f'<div class="sf-xax">{xax}</div>'
        "</div>"
    )


def _feed_table(feed: pd.DataFrame) -> str:
    times = _naive_utc(feed["occurred_at"]).dt.strftime("%H:%M:%S")
    rows = []
    for t, (_, r) in zip(times, feed.iterrows()):
        status = str(r["status"])
        color = STATUS_COLORS.get(status.upper(), NEUTRAL)
        rows.append(
            "<tr>"
            f"<td>{t}</td>"
            f"<td>{_e(r['customer_id'])}</td>"
            f"<td>{_e(r['merchant'])}</td>"
            f"<td class='num'>{_e(_format_minor(r['amount_minor'], r['currency']))}</td>"
            f"<td><span class='sf-st' style='background:{color}'></span>{_e(status.capitalize())}</td>"
            "</tr>"
        )
    return (
        '<div class="sf-fill"><table class="sf-table"><thead><tr>'
        "<th>Time</th><th>Customer</th><th>Merchant</th>"
        '<th class="num">Amount</th><th>Status</th>'
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>"
    )


def _alert_list(df: pd.DataFrame) -> str:
    times = _naive_utc(df["detected_at"]).dt.strftime("%H:%M:%S")
    items = []
    for t, (_, r) in zip(times, df.iterrows()):
        severity = str(r["severity"]).upper()
        color = SEVERITY_COLORS.get(severity, MUTED)
        rule = str(r["rule_code"]).replace("_", " ").capitalize()
        items.append(
            f'<div class="sf-alert" style="--sev:{color}">'
            '<div class="sf-alert-top">'
            f'<span class="sf-sev">{_e(severity.capitalize())}</span>'
            f'<span class="sf-rule" title="{_e(r["rule_code"])}">{_e(rule)}</span>'
            f'<span class="sf-when">{t}</span>'
            "</div>"
            f'<div class="sf-alert-desc">{_e(r["description"])}</div>'
            f'<div class="sf-alert-meta">Customer {_e(r["customer_id"])}</div>'
            "</div>"
        )
    return '<div class="sf-fill">' + "".join(items) + "</div>"


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
try:
    totals = queries.kpi_totals()
    alerts = queries.kpi_alert_counts()
except Exception as e:
    st.error(f"Can't reach PostgreSQL. Check that the postgres container is running. ({e})")
    st.stop()

vol = queries.recent_volume_per_minute(limit=60)
mr = queries.merchant_ranking()
pm = queries.payment_method_breakdown()
sb = queries.status_breakdown()
feed = queries.live_transaction_feed(limit=25)
alerts_df = queries.recent_alerts(limit=50)

high = int(alerts["high_alerts"] or 0)
rate = totals["success_rate_pct"]

# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
head = (
    '<header class="sf-head">'
    '<h1 class="sf-title">StreamForge</h1>'
    f'<div class="sf-status">Updated {datetime.now(timezone.utc).strftime("%H:%M:%S")} UTC. '
    f"Refreshes every {REFRESH_MS // 1000} seconds.</div>"
    "</header>"
)

kpis = (
    '<div class="sf-kpis">'
    + _kpi("Transactions", _count(totals["total_transactions"]))
    + _kpi("Volume", _format_minor(totals["successful_amount_minor"]), "Successful payments")
    + _kpi("Success rate", "—" if rate is None else f"{rate}%")
    + _kpi("Failed", _count(totals["failed_transactions"]))
    + _kpi("Active customers", _count(totals["active_customers"]))
    + _kpi(
        "Anomaly alerts",
        _count(alerts["total_alerts"]),
        f"{high} high severity" if high else "None high severity",
        FAIL if high else "",
    )
    + "</div>"
)

if vol.empty:
    vol_panel = _panel(
        "Transactions per minute",
        _empty(
            "No transactions yet. Send a webhook to /v1/webhooks/{provider} "
            "and it will show up here within seconds."
        ),
        _note("Last 60 minutes"),
        "sf-vol",
    )
else:
    keys = (
        '<div class="sf-keys">'
        f'<span class="sf-key"><i style="background:{ACCENT}"></i>All transactions</span>'
        f'<span class="sf-key"><i style="background:{FAIL}"></i>Failed</span>'
        "</div>"
    )
    vol_panel = _panel("Transactions per minute", _volume_chart(vol), keys, "sf-vol")

if mr.empty:
    mer_body = _empty("Merchant totals appear after the first successful payment.")
else:
    mer_body = _bars(
        mr.sort_values("volume_rank").head(8),
        "merchant",
        "total_volume_minor",
        fmt=_format_minor,
    )
mer_panel = _panel("Merchants by volume", mer_body, _note("Successful payments"), "sf-mer")

pm_body = (
    _empty("No payment data yet.")
    if pm.empty
    else _bars(pm.sort_values("total_transactions", ascending=False), "payment_method", "total_transactions")
)
sb_body = (
    _empty("No status data yet.")
    if sb.empty
    else _bars(
        sb.sort_values("cnt", ascending=False),
        "status",
        "cnt",
        color_map=STATUS_COLORS,
        label=lambda s: s.capitalize(),
    )
)
brk_panel = (
    '<div class="sf-brk">'
    + _panel("Payment methods", pm_body)
    + _panel("Transaction status", sb_body)
    + "</div>"
)

fee_panel = _panel(
    "Latest transactions",
    _empty("No transactions yet.") if feed.empty else _feed_table(feed),
    "",
    "sf-fee",
)
ale_panel = _panel(
    "Anomaly alerts",
    _empty("No alerts. The fraud detector checks for new patterns every 30 seconds.")
    if alerts_df.empty
    else _alert_list(alerts_df),
    _note("Most recent first"),
    "sf-ale",
)

_render(
    f"<style>{CSS}</style>"
    '<div class="sf">'
    + head
    + kpis
    + vol_panel
    + mer_panel
    + brk_panel
    + fee_panel
    + ale_panel
    + "</div>"
)