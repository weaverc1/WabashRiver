#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
storm3_tx.py
============
Transmitter for Storm3 → LoRa link.

2025‑08‑19 changes:
- Remove "P=Hutsonville|<utc>|" preamble from DATA frames
- Parse CSV and send only Date,Time,Riverstage,H-340
- Append local board temp (°C) from /sys/class/thermal/thermal_zone0/temp as TC=<degC>
- Keep conditional GET (If-Modified-Since) + gzip handling
- Generous ACK timing (timeout 5s, 0.3s settle, 1.0s retry pause)

Packet format (ASCII):
  TYPE=DATA;SEQ=<n>;D=<MM/DD/YYYY>,<HH:MM:SS>,<riverstage>,<h340>;TC=<celsius>

Example:
  TYPE=DATA;SEQ=13;D=08/19/2025,17:15:00,19.548,0.02;TC=51.6
"""

import io
import os
import re
import sys
import time
import json
import gzip
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone
from contextlib import redirect_stdout
from typing import Optional, Tuple

# Add project root to allow importing 'wabash'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from wabash import common

# ---------------------------
# LoRa / Radio configuration
# ---------------------------
MY_ADDR   = common.ADDR_TX
PEER_ADDR = common.ADDR_RX

ACK_TIMEOUT_S   = 5.0
POST_SEND_PAUSE = 0.30
RETRY_PAUSE_S   = 1.0
MAX_RETRIES     = 3

# ---------------------------
# Storm3 CSV configuration
# ---------------------------
SITE_ID    = "Hutsonville"
STORM_HOST = "172.20.20.20"
CSV_URL    = f"http://{STORM_HOST}/data/{SITE_ID}.csv"  # adjust if your firmware differs

# ---------------------------
# Paths & logging
# ---------------------------
BASE_DIR  = os.path.expanduser("~/storm/transmitter")
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(STATE_DIR, exist_ok=True)

STATE_FILE = os.path.join(STATE_DIR, "state.json")  # tracks seq + last-modified

logger = common.setup_logging("storm3_tx", BASE_DIR)

# ---------------------------
# Small state helpers
# ---------------------------
def _load_state():
    if not os.path.exists(STATE_FILE):
        return {"seq": 0, "if_modified_since": None}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"seq": 0, "if_modified_since": None}

def _save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        logger.warning(f"Failed to persist state: {e}")

# ---------------------------
# Board/HAT temperature (°C)
# ---------------------------
def get_hat_temp_c() -> Optional[float]:
    """
    Uses the same approach shown in the vendor example: read the kernel thermal zone.
    On Raspberry Pi this returns CPU/board temp which is a good proxy for the HAT temp.
    """
    path = "/sys/class/thermal/thermal_zone0/temp"
    try:
        with open(path, "r") as f:
            milli = int(f.read().strip())
        return milli / 1000.0
    except Exception as e:
        logger.warning(f"Could not read board temperature: {e}")
        return None

# ---------------------------
# HTTP: conditional GET
# ---------------------------
def fetch_latest_csv(if_modified_since: Optional[str]) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    req = urllib.request.Request(CSV_URL, method="GET")
    req.add_header("Accept", "text/csv, */*;q=0.1")
    req.add_header("User-Agent", "storm3_tx/1.1")
    req.add_header("Accept-Encoding", "gzip")
    if if_modified_since:
        req.add_header("If-Modified-Since", if_modified_since)

    try:
        with urllib.request.urlopen(req, timeout=6) as resp:
            status = resp.status
            data = resp.read()
            if resp.headers.get("Content-Encoding", "") == "gzip":
                data = gzip.decompress(data)
            last_mod = resp.headers.get("Last-Modified")
            return status, data.decode("utf-8", errors="replace"), last_mod
    except urllib.error.HTTPError as e:
        if e.code == 304:  # Corrected from 34
            return 304, None, None
        logger.warning(f"HTTP error: {e.reason}")
        return None, None, None
    except Exception as e:
        logger.warning(f"HTTP error: {e}")
        return None, None, None

# ---------------------------
# CSV parsing
# ---------------------------
def extract_latest_row(csv_text: str) -> Optional[Tuple[str, str, str, str]]:
    """
    Returns (date, time, riverstage, h340) from the last non-empty data line.
    Assumes header like:
      Date,Time,Riverstage...,H-340
    """
    if not csv_text:
        return None

    lines = [ln.strip() for ln in csv_text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None

    # find the last line that looks like data (has at least 4 comma-separated fields)
    for ln in reversed(lines):
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) >= 4 and parts[0] and parts[1]:
            date, tm, river, h340 = parts[0], parts[1], parts[2], parts[3]
            return date, tm, river, h340
    return None

# ---------------------------
# Payload formatting
# ---------------------------
def build_payload(seq: int, row: Tuple[str, str, str, str]) -> bytes:
    """
    Final on-air frame (ASCII):
      TYPE=DATA;SEQ=<n>;D=<date>,<time>,<river>,<h340>;TC=<celsius>

    Notes:
      - We keep it compact to fit the LoRa payload budget.
      - Temperature rounded to one decimal if available; omit TC= if not.
    """
    date, tm, river, h340 = row
    tc = get_hat_temp_c()
    if tc is None:
        body = f"TYPE=DATA;SEQ={seq};D={date},{tm},{river},{h340}"
    else:
        body = f"TYPE=DATA;SEQ={seq};D={date},{tm},{river},{h340};TC={tc:.1f}"
    return body.encode("utf-8")

# ---------------------------
# LoRa helpers (header format compatible with RX)
# ---------------------------
ACK_RE = re.compile(r"TYPE=ACK;SEQ=(\d+)\b")

def wait_for_ack(node, expect_seq: int, timeout_s: float) -> bool:
    """
    Poll the radio receive path, parsing the driver’s stdout for ACK frames.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        buf = io.StringIO()
        with redirect_stdout(buf):
            node.receive()
        out = buf.getvalue()
        if out:
            for line in out.splitlines():
                m = re.search(r"message is b'(.*)'", line)
                if not m:
                    continue
                raw = m.group(1)
                try:
                    payload = raw.encode("latin1").decode("unicode_escape")
                except Exception:
                    payload = raw
                a = ACK_RE.search(payload)
                if a:
                    try:
                        got_seq = int(a.group(1))
                        if got_seq == expect_seq:
                            return True
                    except ValueError:
                        pass
        time.sleep(0.02)
    return False

def send_with_ack(node, payload: bytes, seq: int) -> bool:
    header = common.build_header(PEER_ADDR, MY_ADDR)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            node.send(header + payload)
        except Exception as e:
            logger.warning(f"TX error on attempt {attempt}: {e}")
            time.sleep(RETRY_PAUSE_S)
            continue

        logger.info(f"TX seq={seq} attempt={attempt} bytes={len(payload)}")
        time.sleep(POST_SEND_PAUSE)

        if wait_for_ack(node, seq, ACK_TIMEOUT_S):
            logger.info(f"ACK seq={seq} received")
            return True

        logger.warning(f"ACK seq={seq} timeout on attempt {attempt}")
        time.sleep(RETRY_PAUSE_S)

    logger.error(f"Transmission failed after {MAX_RETRIES} retries (seq={seq})")
    return False

# ---------------------------
# Main
# ---------------------------
def main():
    state = _load_state()
    seq = int(state.get("seq", 0)) + 1
    if_modified_since = state.get("if_modified_since")

    # 1) Conditionally fetch CSV
    status, text, last_mod = fetch_latest_csv(if_modified_since)
    if status == 304:
        logger.info("No new data (304 Not Modified)")
        state["seq"] = seq - 1
        _save_state(state)
        return
    if status is None:
        logger.warning("No new data (HTTP error)")
        state["seq"] = seq - 1
        _save_state(state)
        return
    if status != 200 or not text:
        logger.warning("HTTP returned no content")
        state["seq"] = seq - 1
        _save_state(state)
        return

    # 2) Extract latest CSV row
    row = extract_latest_row(text)
    if not row:
        logger.info("No new data (CSV empty or header-only)")
        state["seq"] = seq - 1
        _save_state(state)
        return

    date, tm, river, h340 = row
    logger.info(f"Latest CSV row D={date},{tm},{river},{h340}")

    # 3) Build payload (compact, no site/time preamble)
    payload = build_payload(seq, row)

    # 4) Init radio and send with ACK
    node = common.init_radio(my_addr=MY_ADDR, for_tx=True)
    ok = send_with_ack(node, payload, seq)

    # 5) Persist state
    if ok and last_mod:
        state["if_modified_since"] = last_mod
    state["seq"] = seq  # advance seq after any attempted send
    _save_state(state)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
