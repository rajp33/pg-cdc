#!/usr/bin/env python3
import argparse
import json
import os
from xml.sax.saxutils import escape

THEMES = {
    "light": {"canvas": "#ffffff", "ink": "#1f2328", "ink2": "#52514e", "muted": "#6e6c66", "axis": "#c3c2b7",
              "accent": "#2a78d6", "other": "#898781", "companion": "#c3c2b7"},
    "dark": {"canvas": "#0d1117", "ink": "#f0efec", "ink2": "#c3c2b7", "muted": "#9a988f", "axis": "#383835",
             "accent": "#3987e5", "other": "#898781", "companion": "#52514e"},
}
ROWS = [("pg-cdc", "pg-cdc"), ("sequin", "Sequin"), ("debezium", "Debezium Server"), ("pgcdc", "pgcdc (webhook)")]
FONT = "system-ui, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
LABEL_W, PANEL_W, GAP, TOP, PITCH, BAR = 150, 196, 26, 62, 30, 16
WIDTH = LABEL_W + 3 * PANEL_W + 2 * GAP + 10
TIP_ROOM = 92


def fmt(v, unit):
    return f"{v:,.0f} {unit}" if v >= 10 else f"{v:,.1f} {unit}"


def bar(x, y, w, color, round_end=True):
    r = min(4, w / 2) if round_end else 0
    if r <= 0:
        return f'<rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="{BAR}" fill="{color}"/>'
    return (f'<path d="M{x:.1f},{y} h{w - r:.1f} a{r},{r} 0 0 1 {r},{r} v{BAR - 2 * r} a{r},{r} 0 0 1 -{r},{r} h-{w - r:.1f} z" '
            f'fill="{color}"/>')


def panel(i, title, unit, values, theme, cap=None, notes=None, companions=None):
    t = THEMES[theme]
    x0 = LABEL_W + i * (PANEL_W + GAP)
    known = [v for v in values.values() if v is not None]
    top = min(max(known), cap) if cap else max(known)
    if companions:
        top = max(top, *(values[k] + c for k, c in companions.items()))
    scale = (PANEL_W - TIP_ROOM) / top
    out = [f'<text x="{x0}" y="22" font-size="13" font-weight="600" fill="{t["ink"]}">{escape(title[0])}</text>',
           f'<text x="{x0}" y="40" font-size="12" fill="{t["muted"]}">{escape(title[1])}</text>',
           f'<line x1="{x0}" y1="{TOP - 8}" x2="{x0}" y2="{TOP + len(ROWS) * PITCH - 6}" stroke="{t["axis"]}" stroke-width="1"/>']
    for row, (key, _) in enumerate(ROWS):
        y = TOP + row * PITCH
        v = values.get(key)
        color = t["accent"] if key == "pg-cdc" else t["other"]
        if v is None:
            note = (notes or {}).get(key, "")
            out.append(f'<text x="{x0 + 8}" y="{y + 12}" font-size="11.5" fill="{t["muted"]}">{escape(note)}</text>')
            continue
        off_scale = cap is not None and v > cap
        w = (cap if off_scale else v) * scale
        out.append(bar(x0, y, w, color, round_end=not (companions and key in companions)))
        label = fmt(v / 1000, "s") if unit == "ms" and v >= 1000 else fmt(v, unit)
        end = x0 + w
        if companions and key in companions:
            cw = companions[key] * scale
            out.append(bar(end + 2, y, cw - 2, t["companion"]))
            end += cw
            label = f"{v:,.0f} + {companions[key]:,.0f} {unit}"
        if off_scale:
            bx = x0 + w - 14
            out.append(f'<path d="M{bx},{y + BAR + 2} l6,-{BAR + 4} M{bx + 5},{y + BAR + 2} l6,-{BAR + 4}" stroke="{t["canvas"]}" stroke-width="2.5"/>')
            label += (notes or {}).get(key, "")
        weight = ' font-weight="600"' if key == "pg-cdc" else ""
        out.append(f'<text x="{end + 6:.1f}" y="{y + 12}" font-size="12"{weight} fill="{t["ink2"] if key != "pg-cdc" else t["ink"]}">{escape(label)}</text>')
    return out


def render(data, theme, footnotes):
    t = THEMES[theme]
    height = TOP + len(ROWS) * PITCH + 14 + 17 * len(footnotes)
    body = []
    for row, (key, name) in enumerate(ROWS):
        y = TOP + row * PITCH + 12
        weight = ' font-weight="600"' if key == "pg-cdc" else ""
        body.append(f'<text x="{LABEL_W - 14}" y="{y}" font-size="13" text-anchor="end"{weight} fill="{t["ink"]}">{escape(name)}</text>')
    get = lambda k: {key: (data.get(key) or {}).get(k) for key, _ in ROWS}
    body += panel(0, ("CPU per event", "500k-row backlog drain, µs"), "µs", get("backlog_cpu_us_per_event"), theme)
    lat = get("latency3000_p50_ms")
    lat["pgcdc"] = None
    others = [v for k, v in lat.items() if v is not None and k != "debezium"]
    cap = max(others) * 1.15 if lat.get("debezium") and lat["debezium"] > 3 * max(others) else None
    body += panel(1, ("Latency p50 at 3,000 rows/s", "commit to SNS acknowledgement"), "ms", lat, theme, cap=cap,
                  notes={"pgcdc": "not comparable: HTTP sink", "debezium": ", fell behind"})
    rss = get("idle_rss_anon_mib")
    comp = (data.get("sequin") or {}).get("idle_rss_companions_anon_mib")
    body += panel(2, ("Idle memory", "anonymous RSS, MiB"), "MiB", rss, theme, companions={"sequin": comp} if comp else None)
    for n, line in enumerate(footnotes):
        body.append(f'<text x="{LABEL_W}" y="{TOP + len(ROWS) * PITCH + 16 + 17 * n}" font-size="11.5" fill="{t["muted"]}">{escape(line)}</text>')
    desc = "pg-cdc compared with Sequin, Debezium Server and pgcdc. " + " ".join(footnotes)
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" viewBox="0 0 {WIDTH} {height}" '
            f'font-family="{FONT}" role="img" aria-labelledby="t d">\n<title id="t">CDC benchmark: CPU per event, latency and idle memory</title>\n'
            f'<desc id="d">{escape(desc)}</desc>\n' + "\n".join(body) + "\n</svg>\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data", help="output of summarize.py --json")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs"))
    a = ap.parse_args()
    data = json.load(open(a.data))
    footnotes = ["Lower is better. One machine, SNS on moto. CPU and latency: mean of two runs; memory: median of six.",
                 "Sequin's second memory segment is its own Postgres and Redis."]
    for theme in THEMES:
        with open(os.path.join(a.out, f"benchmarks-{theme}.svg"), "w") as f:
            f.write(render(data, theme, footnotes))


if __name__ == "__main__":
    main()
