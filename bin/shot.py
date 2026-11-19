#!/usr/bin/env python3
"""Снимки вывода команд в виде окна терминала: python bin/shot.py имя=out/файл.txt[:с-по] ...

с-по: номера строк файла, по умолчанию весь файл.
"""
import html
import sys

from playwright.sync_api import sync_playwright

PAGE = """<html><body style="margin:0;background:#1e1e1e">
<div style="background:#3c3c3c;color:#ccc;font:13px sans-serif;padding:6px 12px">~/DE-itmo/hw4</div>
<pre style="margin:0;padding:10px 14px;color:#d4d4d4;font:13px/1.35 'DejaVu Sans Mono',monospace;white-space:pre-wrap">{}</pre>
</body></html>"""


def render(lines):
    out = []
    for line in lines:
        s = html.escape(line.rstrip("\n"))
        if s.startswith("$ "):
            s = f'<span style="color:#6a9955">$</span> <span style="color:#dcdcaa">{s[2:]}</span>'
        out.append(s)
    return PAGE.format("\n".join(out))


with sync_playwright() as p:
    b = p.chromium.launch(channel="chrome")
    pg = b.new_page(viewport={"width": 1100, "height": 200}, device_scale_factor=1.5)
    for arg in sys.argv[1:]:
        name, spec = arg.split("=", 1)
        path, _, rng = spec.partition(":")
        lines = open(path, encoding="utf-8").readlines()
        if rng:
            lo, hi = map(int, rng.split("-"))
            lines = lines[lo - 1:hi]
        pg.set_content(render(lines))
        pg.screenshot(path=f"report/shots/{name}.png", full_page=True)
        print(f"report/shots/{name}.png")
    b.close()
