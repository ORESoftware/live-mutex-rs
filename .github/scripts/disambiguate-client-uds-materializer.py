#!/usr/bin/env python3
from pathlib import Path

path = Path("src/client.rs")
text = path.read_text(encoding="utf-8")
marker = "    pub async fn connect_uds(\n"
positions = []
start = 0
while True:
    index = text.find(marker, start)
    if index < 0:
        break
    positions.append(index)
    start = index + len(marker)
if len(positions) != 2:
    raise SystemExit(f"expected two client UDS entry points, got {len(positions)}")
second = positions[1]
text = (
    text[:second]
    + "    pub async fn\n        connect_uds(\n"
    + text[second + len(marker):]
)
path.write_text(text, encoding="utf-8")
