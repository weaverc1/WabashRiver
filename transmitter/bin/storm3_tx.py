#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
storm3_tx.py
================

This script runs on the Raspberry Pi attached to a WaterLOG Storm 3
data logger.  It periodically fetches the most recent row from the
Storm 3 CSV and forwards it over a LoRa link to a peer receiver.  The
payload includes the site code, a compact timestamp, the stage and
rain values, a temperature reading from the LoRa HAT (falling back to
CPU temperature), and a monotonically increasing sequence number.

Key features
-------------
* **HTTP fetch with If‑Modified‑Since**:  We store the `Last‑Modified`
  header from the previous run and include it on the next fetch.  If
  nothing has changed the server returns `304 Not Modified` and we
  avoid parsing or transmitting duplicate data.
* **CSV banner detection**:  The Storm 3 often prepends a banner
  before the CSV header.  We search for a line that contains
  `Date` and `Time` with a comma and treat that as the header.
* **Idempotent sends**:  We remember the last timestamp we sent in a
  small JSON state file.  If the newest CSV timestamp is not newer
  than our last send we exit quietly.  This prevents duplicate
  transmissions after a reboot.
* **LoRa transmission with ACKs**:  Each packet includes a
  sequence number.  After sending we wait for a matching
  `TYPE=ACK;SEQ=<n>` response from the peer.  We retry a few times
  before giving up.  Only when an ACK is received do we bump the
  sequence and last‑sent timestamp in the state file.
* **Rotating logs**:  The script writes a structured log to
  ``$BASE_DIR/logs/tx.log``.  The log files rotate at 200 KiB to
  avoid filling the SD card.

Usage
-----
This file is designed to be invoked by a systemd timer.  See the
companion unit files for an example.  You can also test it manually:

.. code-block:: bash

   python3 storm3_tx.py

Environment
-----------
The script assumes the `sx126x` driver module is installed and that
the LoRa HAT is connected to ``/dev/serial0``.  Adjust the
configuration section below if your hardware differs.  The script
requires Python 3.7+.
"""

import csv
import io
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Attempt to import the sx126x LoRa driver.  This import will fail
# during static analysis or on systems without the hardware.  The
# script only requires the module at run time on the Pi.
try:
    import sx126x  # type: ignore
except ImportError:
    sx126x = None  # type: ignore

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Site code used in the payload.  Keep it short to save airtime.
SITE_CODE = "HUT"

# IP address of the Storm 3.  The CSV path is fixed beneath ``/data``.
STORM_IP = "172.20.20.20"
CSV_URL = f"http://{STORM_IP}/data/Hutsonville.csv"

# HTTP request timeout in seconds.  Keep it short to prevent hangs.
HTTP_TIMEOUT_S = 5

# LoRa radio parameters.  These match the working values you used in
# your range tests.  If you change the frequency or air speed on the
# receiver side, update these to match.
SERIAL_PORT = "/dev/serial0"
MY_ADDR = 1
PEER_ADDR = 2
FREQ_MHZ = 915
POWER_DBM = 22
AIR_SPEED = 1200

# Acknowledgement configuration.  Each packet will be retried up to
# `MAX_RETRIES` times.  After each send we wait `ACK_TIMEOUT_S`
# seconds for a matching ACK.
ACK_TIMEOUT_S = 2.0
MAX_RETRIES = 3

# Directory structure.  All state and log files live under
# ``BASE_DIR`` to allow for simple backups and management.
BASE_DIR = os.path.expanduser("~/storm/transmitter")
LOG_DIR = os.path.join(BASE_DIR, "logs")
STATE_PATH = os.path.join(BASE_DIR, "state.json")
LM_PATH = os.path.join(BASE_DIR, "last_modified.txt")
LOG_PATH = os.path.join(LOG_DIR, "tx.log")

# Configure logging.  We rotate the log after 200 KiB and keep three
# backups.  The timestamps are recorded in UTC for consistency.
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("storm3_tx")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_PATH, maxBytes=200_000, backupCount=3)
formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
formatter.converter = time.gmtime  # store times as UTC
handler.setFormatter(formatter)
logger.addHandler(handler)


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
def load_state() -> dict:
    """Load the persistent state from disk.

    The state file stores the last timestamp we sent and the last
    sequence number used.  If the file does not exist or cannot be
    parsed we return sensible defaults.
    """
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {"last_sent_ts": None, "seq": 0}


def save_state(state: dict) -> None:
    """Atomically persist the state to disk.

    We write to a temporary file and then replace the existing state
    file to avoid leaving a partially written file on interruption.
    """
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)


def freq_offset(freq_mhz: int) -> int:
    """Convert a frequency in MHz to the offset byte used by the SX126x.

    The driver interprets the offset as (freq – 850 MHz) for high bands
    or (freq – 410 MHz) for low bands.  See your range test script for
    reference.
    """
    return int(freq_mhz - (850 if freq_mhz > 850 else 410))


def build_header(dst_addr: int, dst_off: int, src_addr: int, src_off: int) -> bytes:
    """Build a 6‑byte SX126x header.

    The header format is:

    * 2 bytes: destination address (big endian)
    * 1 byte : destination frequency offset
    * 2 bytes: source address (big endian)
    * 1 byte : source frequency offset
    """
    return bytes([
        (dst_addr >> 8) & 0xFF,
        dst_addr & 0xFF,
        dst_off & 0xFF,
        (src_addr >> 8) & 0xFF,
        src_addr & 0xFF,
        src_off & 0xFF,
    ])


def cpu_temp_c() -> float:
    """Return the Raspberry Pi CPU temperature in °C.

    If the temperature cannot be read a NaN is returned instead of
    raising an exception.
    """
    try:
        with open("/sys/class/thermal/thermal_zone0/temp") as f:
            return float(f.read().strip()) / 1000.0
    except Exception:
        return float("nan")


def lora_hat_temp_c(node) -> float:
    """Attempt to read the LoRa HAT temperature.

    The sx126x driver is not uniformly documented.  To maximise
    compatibility we probe several likely attribute names on the
    ``node`` object.  If none are found we fall back to the CPU
    temperature.  Additional names can be added here as needed.
    """
    for name in ("get_temp", "get_temperature", "chip_temp", "temperature"):
        if hasattr(node, name):
            try:
                val = getattr(node, name)()
                if val is not None:
                    return float(val)
            except Exception:
                pass
    return cpu_temp_c()


def parse_storm3_csv(body: bytes) -> tuple:
    """Extract the last timestamp, stage and rain values from CSV bytes.

    The Storm 3 returns a banner line followed by a header and then
    rows.  We locate the header by looking for a line containing
    "Date", "Time" and a comma.  From that header we derive
    column indices for the date, time, stage and rain fields.  If
    stage or rain cannot be located by heuristic we default to the
    third and fourth columns.  We return a tuple
    ``(ts_compact, stage, rain)`` where ``ts_compact`` is of the
    form ``YYYYMMDDTHHMMZ``.  If no rows are present a
    ``RuntimeError`` is raised.
    """
    text = body.decode("utf-8", errors="ignore").splitlines()
    if not text:
        raise RuntimeError("CSV body is empty")

    # Locate the header within the first few lines.  The Storm
    # sometimes includes a banner line before the header.
    header_idx = 0
    for i, line in enumerate(text[:5]):
        if "," in line and "Date" in line and "Time" in line:
            header_idx = i
            break

    reader = csv.reader(io.StringIO("\n".join(text[header_idx:])))
    header = next(reader, None)
    if not header:
        raise RuntimeError("Missing CSV header")
    rows = [row for row in reader if row and len(row) >= 3]
    if not rows:
        raise RuntimeError("No CSV data rows")
    last = rows[-1]

    # Build a map of column names to indices
    colmap = {name.strip(): idx for idx, name in enumerate(header)}
    i_date = colmap.get("Date", 0)
    i_time = colmap.get("Time", 1)
    i_stage = None
    i_rain = None
    for name, idx in colmap.items():
        lower = name.lower()
        if i_stage is None and ("stage" in lower or "parameter1" in lower):
            i_stage = idx
        if i_rain is None and ("h-340" in lower or "rain" in lower):
            i_rain = idx
    if i_stage is None:
        i_stage = 2
    if i_rain is None:
        i_rain = 3

    date_s = last[i_date].strip()
    time_s = last[i_time].strip()
    timestamp_local = f"{date_s} {time_s}"
    # Try multiple date formats.  Storm 3 usually uses MM/DD/YYYY.
    dt = None
    for fmt in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(timestamp_local, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        # If parsing fails, fall back to current UTC time.
        dt = datetime.utcnow()

    # Format as YYYYMMDDTHHMMZ (minute precision).  We treat the
    # logger’s local time as UTC here.  The peer can translate as
    # needed.
    ts_compact = dt.strftime("%Y%m%dT%H%MZ")
    stage = last[i_stage].strip()
    rain = last[i_rain].strip()
    return ts_compact, stage, rain


def fetch_latest_row() -> tuple:
    """Fetch the CSV from the Storm 3 and return the newest row.

    Returns a tuple ``(ts_compact, stage, rain, last_modified)``.
    If the server responds with 304 Not Modified we return
    ``(None, None, None, last_modified)`` to signal no update.
    """
    # Read the previous Last‑Modified if present
    ims = None
    if os.path.exists(LM_PATH):
        try:
            ims = open(LM_PATH, "r").read().strip()
        except Exception:
            ims = None

    # Build request with optional If‑Modified‑Since header
    req = urllib.request.Request(CSV_URL, method="GET")
    if ims:
        req.add_header("If-Modified-Since", ims)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            status = resp.status
            body = resp.read()
            last_mod = resp.headers.get("Last-Modified")
    except urllib.error.HTTPError as e:
        status = e.code
        body = b""
        last_mod = None
    except Exception as e:
        logger.warning(f"HTTP error: {e}")
        return None, None, None, ims

    if status == 304:
        return None, None, None, ims
    if status != 200 or not body:
        raise RuntimeError(f"Failed to fetch CSV: HTTP {status}")

    ts_compact, stage, rain = parse_storm3_csv(body)

    # Persist the Last‑Modified header for the next run
    if last_mod:
        try:
            with open(LM_PATH, "w") as f:
                f.write(last_mod)
        except Exception:
            pass

    return ts_compact, stage, rain, last_mod


def wait_for_ack(node, expected_seq: int, deadline: float) -> bool:
    """Block until an ACK with the expected sequence is seen or timeout.

    The sx126x driver prints to stdout on receive events.  We capture
    its output via a string buffer and search for payloads that look
    like ``TYPE=ACK;SEQ=<n>`` with the matching sequence number.
    """
    pattern = re.compile(r"message is b'(.*)'")
    while time.time() < deadline:
        from contextlib import redirect_stdout
        import io as _io
        buf = _io.StringIO()
        with redirect_stdout(buf):
            node.receive()
        out = buf.getvalue()
        if out:
            for line in out.splitlines():
                m = pattern.search(line)
                if m:
                    try:
                        payload = m.group(1).encode("latin1").decode("unicode_escape")
                    except Exception:
                        payload = m.group(1)
                    if "TYPE=ACK" in payload:
                        m2 = re.search(r"SEQ=(\d+)", payload)
                        if m2 and int(m2.group(1)) == expected_seq:
                            return True
        time.sleep(0.05)
    return False


def send_with_ack(node, peer_addr: int, payload: bytes, seq: int) -> bool:
    """Transmit a payload and wait for its acknowledgement.

    We build the LoRa header and raw frame and send it.  If the
    acknowledgement is not received within the timeout we retry.  The
    number of retries is limited by `MAX_RETRIES`.
    """
    dst_off = freq_offset(FREQ_MHZ)
    src_off = freq_offset(FREQ_MHZ)
    frame = build_header(peer_addr, dst_off, MY_ADDR, src_off) + payload
    for attempt in range(1, MAX_RETRIES + 1):
        node.send(frame)
        logger.info(f"TX seq={seq} attempt={attempt} bytes={len(payload)}")
        if wait_for_ack(node, seq, time.time() + ACK_TIMEOUT_S):
            logger.info(f"ACK seq={seq} received")
            return True
        logger.warning(f"ACK seq={seq} timeout on attempt {attempt}")
    return False


def main() -> int:
    """Main entry point.  Fetch the latest Storm 3 row and send it.

    Returns zero on success, non‑zero on failure.  On failure we log
    the error but do not update the state file, so the next run will
    attempt again.
    """
    # Ensure the base directory exists
    os.makedirs(BASE_DIR, exist_ok=True)

    # Load the persistent state
    state = load_state()

    # Fetch the newest CSV row
    try:
        ts_compact, stage, rain, _ = fetch_latest_row()
    except Exception as e:
        logger.error(f"Fetch/parsing error: {e}")
        return 1

    # If nothing has changed, exit quietly
    if ts_compact is None:
        logger.info("No new data (304 Not Modified)")
        return 0

    # Check for duplicate sends
    last_sent_ts = state.get("last_sent_ts")
    if last_sent_ts and ts_compact <= last_sent_ts:
        logger.info(f"Skipping send (ts={ts_compact} <= last_sent_ts={last_sent_ts})")
        return 0

    # Initialize the radio
    if sx126x is None:
        logger.error("sx126x module not installed; cannot send")
        return 2
    try:
        node = sx126x.sx126x(serial_num=SERIAL_PORT,
                             freq=FREQ_MHZ,
                             addr=MY_ADDR,
                             power=POWER_DBM,
                             rssi=True,
                             air_speed=AIR_SPEED,
                             relay=False)
    except Exception as e:
        logger.error(f"Radio init failed: {e}")
        return 3

    # Read the LoRa HAT temperature (falls back to CPU)
    temp_c = lora_hat_temp_c(node)

    # Bump the sequence number
    seq = int(state.get("seq", 0)) + 1

    # Build the payload.  We prefix with a small header (TYPE and SEQ)
    # and embed the data in a pipe‑separated field for easy parsing.
    payload_str = f"{SITE_CODE}|{ts_compact}|{stage}|{rain}|{temp_c:.2f}|{seq}"
    payload = f"TYPE=DATA;SEQ={seq};P={payload_str}".encode()

    # Send and wait for acknowledgement
    success = send_with_ack(node, PEER_ADDR, payload, seq)
    if not success:
        logger.error(f"Transmission failed after {MAX_RETRIES} retries (seq={seq})")
        return 4

    # Persist the new state
    state["last_sent_ts"] = ts_compact
    state["seq"] = seq
    save_state(state)
    logger.info(f"Transmission successful: ts={ts_compact} stage={stage} rain={rain} tempC={temp_c:.2f} seq={seq}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        sys.exit(130)
