#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
storm3_tx.py
============
Transmitter for Storm3 → LoRa link.

Changes (2025-08-18):
- Increase ACK timeout to 5s
- Add 0.3s post-send settle time before listening for ACK
- Add 1.0s pause between retries
- Keep conditional-fetch (If-Modified-Since) behavior; log 304 as "No new data"

This script:
1) Conditionally fetches the latest CSV row from the Storm3 over HTTP.
2) If new data is available, formats a compact payload and sends via LoRa.
3) Waits for TYPE=ACK;SEQ=<n> from the receiver with generous timing.
4) Logs everything to a rotating tx.log.

Assumptions:
- sx126x driver (Waveshare SX126x HAT) is installed.
- UART is /dev/serial0 on a Pi.
- RX is configured as MY_ADDR=2, TX is MY_ADDR=1.
- Frequency 915 MHz, air speed 1200 bps (must match RX).
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
from logging.handlers import RotatingFileHandler
from contextlib import redirect_stdout


# ---------------------------
# LoRa / Radio configuration
# ---------------------------
try:
    import sx126x  # type: ignore
except ImportError:
    sx126x = None  # type: ignore

SERIAL_PORT = "/dev/serial0"
FREQ_MHZ    = 915
AIR_SPEED   = 1200
POWER_DBM   = 22

MY_ADDR   = 1  # transmitter
PEER_ADDR = 2  # receiver

# Generous, “chill” timings
ACK_TIMEOUT_S   = 5.0   # wait up to 5 seconds for ACK (was ~2s)
POST_SEND_PAUSE = 0.30  # short settle before listening for ACK
RETRY_PAUSE_S   = 1.0   # spacing between retries
MAX_RETRIES     = 3

# ---------------------------
# Storm3 CSV configuration
# ---------------------------
SITE_ID    = "Hutsonville"
STORM_HOST = "172.20.20.20"

# Adjust path if your firmware serves CSV elsewhere; this is your proven path:
CSV_URL      = f"http://{STORM_HOST}/data/{SITE_ID}.csv"

# ---------------------------
# Paths & logging
# ---------------------------
BASE_DIR = os.path.expanduser("~/storm/transmitter")
LOG_DIR  = os.path.join(BASE_DIR, "logs")
STATE_DIR= os.path.join(BASE_DIR, "state")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

LOG_PATH = os.path.join(LOG_DIR, "tx.log")
STATE_FILE = os.path.join(STATE_DIR, "state.json")  # tracks seq + last-modified

logger = logging.getLogger("storm3_tx")
logger.setLevel(logging.INFO)
_handler = RotatingFileHandler(LOG_PATH, maxBytes=300_000, backupCount=3)
_fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
_fmt.converter = time.gmtime
_handler.setFormatter(_fmt)
logger.addHandler(_handler)

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

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")















# ---------------------------
# HTTP: conditional GET
# ---------------------------
def fetch_latest_csv(if_modified_since: str | None):
    req = urllib.request.Request(CSV_URL, method="GET")
    req.add_header("Accept", "text/csv, */*;q=0.1")
    req.add_header("User-Agent", "storm3_tx/1.0")
    req.add_header("Accept-Encoding", "gzip")

    if if_modified_since:
        req.add_header("If-Modified-Since", if_modified_since)

    try:
        with urllib.request.urlopen(req, timeout=6) as resp:
            status = resp.status
            # Handle gzip
            data = resp.read()
            if resp.headers.get("Content-Encoding","") == "gzip":
                data = gzip.decompress(data)
            last_mod = resp.headers.get("Last-Modified")
            return status, data.decode("utf-8", errors="replace"), last_mod
    except urllib.error.HTTPError as e:
        if e.code == 304:
            # Not modified
            return 304, None, None
        logger.warning(f"HTTP error: {e.reason}")
        return None, None, None
    except Exception as e:
        logger.warning(f"HTTP error: {e}")
        return None, None, None

# ---------------------------
# CSV parsing
# ---------------------------
def extract_latest_line(csv_text: str) -> str | None:
    # Assumes first line is header; return last non-empty line




    if not csv_text:
        return None

    lines = [ln.strip() for ln in csv_text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    return lines[-1]








# ---------------------------
# Payload formatting
# ---------------------------
def build_payload(seq: int, latest_line: str) -> bytes:
    """
    Keep compact, TX-only payload. We’ll embed site ID, UTC timestamp of send,
    and the raw CSV last line for traceability. Your receiver already logs RSSI.




    """
    send_ts = _now_iso()
    # Trim the CSV line if it’s too long; LoRa payload budget is small
    # (We saw bytes=55 in your logs; keep things lean)
    csv_snippet = latest_line
    if len(csv_snippet) > 24:
        csv_snippet = csv_snippet[:24]

    body = f"TYPE=DATA;SEQ={seq};P={SITE_ID}|{send_ts}|{csv_snippet}"
    return body.encode("utf-8")

# ---------------------------
# LoRa helpers (header format compatible with RX)
# ---------------------------
def freq_offset(mhz: int) -> int:
    return int(mhz - (850 if mhz > 850 else 410))

def build_header(dst_addr: int, dst_off: int, src_addr: int, src_off: int) -> bytes:
    return bytes([
        (dst_addr >> 8) & 0xFF, dst_addr & 0xFF, dst_off & 0xFF,
        (src_addr >> 8) & 0xFF, src_addr & 0xFF, src_off & 0xFF,
    ])

def init_radio():
    if sx126x is None:
        logger.error("sx126x module not installed; cannot transmit")
        sys.exit(2)
    try:
        node = sx126x.sx126x(
            serial_num=SERIAL_PORT,
            freq=FREQ_MHZ,
            addr=MY_ADDR,
            power=POWER_DBM,
            rssi=False,        # TX doesn’t need RSSI
            air_speed=AIR_SPEED,
            relay=False
        )
        return node
    except Exception as e:
        logger.error(f"Radio init failed: {e}")
        sys.exit(3)

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
            # Look for the "message is b'...'" line and decode
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
    header = build_header(PEER_ADDR, freq_offset(FREQ_MHZ),
                          MY_ADDR,  freq_offset(FREQ_MHZ))
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            node.send(header + payload)
        except Exception as e:
            logger.warning(f"TX error on attempt {attempt}: {e}")
            time.sleep(RETRY_PAUSE_S)
            continue

        logger.info(f"TX seq={seq} attempt={attempt} bytes={len(payload)}")

        # Let the RX parse & respond before we start listening hard
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
def run_single_transmission():
    state = _load_state()
    seq = int(state.get("seq", 0)) + 1
    if_modified_since = state.get("if_modified_since")

    # 1) Conditionally fetch CSV
    status, text, last_mod = fetch_latest_csv(if_modified_since)
    if status == 304:
        logger.info("No new data (304 Not Modified)")
        # Keep current state.seq unchanged on no-send? We can, but your log kept seq at 1.
        # We'll not bump seq when no send happens:
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

    # 2) Extract latest CSV line
    latest = extract_latest_line(text)
    if not latest:
        logger.info("No new data (CSV empty or header-only)")
        state["seq"] = seq - 1
        _save_state(state)
        return

    # 3) Build payload
    payload = build_payload(seq, latest)




    # 4) Init radio and send with ACK
    node = init_radio()
    ok = send_with_ack(node, payload, seq)

    # 5) Persist state
    if ok and last_mod:
        state["if_modified_since"] = last_mod
    # Only advance seq if we actually transmitted (regardless of ACK success) to avoid reuse
    state["seq"] = seq
    _save_state(state)

def main():
    """
    Main loop to run the transmitter as a service.
    """
    logger.info("Starting Storm3 LoRa transmitter service.")
    while True:
        try:
            logger.info("Starting new transmission cycle.")
            run_single_transmission()
            logger.info("Transmission cycle finished. Sleeping for 15 minutes.")
        except Exception as e:
            logger.exception(f"An error occurred in the main loop: {e}")
            logger.info("Sleeping for 1 minute before retrying.")
            time.sleep(60)
            continue

        time.sleep(900) # 15 minutes

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Transmitter stopped by user.")
        print("\nTransmitter stopped.")
