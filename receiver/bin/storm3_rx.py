#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
storm3_rx.py
=================

LoRa receiver for Storm3 → Pi with ACK support and robust driver error handling.

- Logs payloads + RSSI to rx.log and rx.csv (rotating).
- Immediately ACKs TYPE=DATA packets with TYPE=ACK;SEQ=<n>.
- Wraps sx126x.receive() in a try/except to survive occasional driver IndexErrors.
"""

import io
import logging
import os
import re
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from contextlib import redirect_stdout

try:
    import sx126x  # type: ignore
except ImportError:
    sx126x = None  # type: ignore

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERIAL_PORT = "/dev/serial0"   # UART for the LoRa HAT
MY_ADDR = 2                     # This node’s address (receiver)
PEER_ADDR = 1                   # Transmitter’s address (for ACKs)
FREQ_MHZ = 915                  # Must match the transmitter
POWER_DBM = 22                  # Required by driver (not used for RX)
AIR_SPEED = 1200                # Must match the transmitter

BASE_DIR = os.path.expanduser("~/storm/receiver")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_PATH = os.path.join(LOG_DIR, "rx.log")
CSV_PATH = os.path.join(LOG_DIR, "rx.csv")

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Set up logging with rotation to avoid filling the disk
logger = logging.getLogger("storm3_rx")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_PATH, maxBytes=200_000, backupCount=3)
formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
formatter.converter = time.gmtime
handler.setFormatter(formatter)
logger.addHandler(handler)

# If the CSV log does not exist, write the header
if not os.path.exists(CSV_PATH):
    with open(CSV_PATH, "w") as f:
        f.write("iso_time,packet_rssi,noise_rssi,payload\n")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def ensure_driver() -> None:
    """Abort if the sx126x driver is missing."""
    if sx126x is None:
        logger.error("sx126x module not installed; cannot receive")
        sys.exit(2)


def now_iso() -> str:
    """Return the current time in ISO‑8601 format with UTC suffix."""
    return datetime.utcnow().isoformat() + "Z"


def freq_offset(freq_mhz: int) -> int:
    """Convert frequency to the offset byte used by the SX126x header."""
    return int(freq_mhz - (850 if freq_mhz > 850 else 410))


def build_header(dst_addr: int, dst_off: int, src_addr: int, src_off: int) -> bytes:
    """Build the 6‑byte header: dst(2) dst_off(1) src(2) src_off(1)."""
    return bytes([
        (dst_addr >> 8) & 0xFF, dst_addr & 0xFF, dst_off & 0xFF,
        (src_addr >> 8) & 0xFF, src_addr & 0xFF, src_off & 0xFF,
    ])


def send_ack(node, seq: int) -> None:
    """Send TYPE=ACK;SEQ=<n> back to the transmitter."""
    ack_payload = f"TYPE=ACK;SEQ={seq}".encode()
    hdr = build_header(
        PEER_ADDR, freq_offset(FREQ_MHZ),
        MY_ADDR,   freq_offset(FREQ_MHZ),
    )
    try:
        node.send(hdr + ack_payload)
        logger.info(f"ACK sent seq={seq}")
    except Exception as e:
        logger.warning(f"ACK send failed for seq={seq}: {e}")


# ---------------------------------------------------------------------------
# Main receive loop
# ---------------------------------------------------------------------------
def listen() -> None:
    """Initialise the radio and print/log packets. Send ACKs for DATA."""
    ensure_driver()
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
        sys.exit(3)

    print("LoRa receiver started. Waiting for packets…")

    # Patterns for parsing the driver stdout
    pattern_msg   = re.compile(r"message is b'(.*)'")
    pattern_prssi = re.compile(r"packet rssi value:\s*(-?\d+)", re.IGNORECASE)
    pattern_nrssi = re.compile(r"noise rssi value:\s*(-?\d+)",  re.IGNORECASE)
    pattern_seq   = re.compile(r"\bSEQ=(\d+)\b")

    while True:
        # Capture the driver’s stdout to parse the receive event
        buf = io.StringIO()
        try:
            with redirect_stdout(buf):
                # Defensive wrapper: some driver versions raise IndexError on noise/short buffers
                node.receive()
        except Exception as e:
            logger.warning(f"Driver receive() error: {e}")
            time.sleep(0.10)
            continue

        out = buf.getvalue()
        if not out:
            time.sleep(0.05)
            continue

        # Initialise fields for this packet
        payload_raw = None
        packet_rssi = None
        noise_rssi  = None

        for line in out.splitlines():
            # Extract the payload as a bytes literal string
            m_msg = pattern_msg.search(line)
            if m_msg:
                payload_raw = m_msg.group(1)

            m_prssi = pattern_prssi.search(line)
            if m_prssi:
                try:
                    packet_rssi = int(m_prssi.group(1))
                except ValueError:
                    packet_rssi = None

            m_nrssi = pattern_nrssi.search(line)
            if m_nrssi:
                try:
                    noise_rssi = int(m_nrssi.group(1))
                except ValueError:
                    noise_rssi = None

        if payload_raw is None:
            # Nothing parsable this cycle (driver might have printed only meta lines)
            time.sleep(0.02)
            continue

        # Decode any escape sequences in the raw payload
        try:
            payload = payload_raw.encode("latin1").decode("unicode_escape")
        except Exception:
            payload = payload_raw

        # If it's a DATA packet with SEQ=n, send an ACK back immediately
        if "TYPE=DATA" in payload:
            m_seq = pattern_seq.search(payload)
            if m_seq:
                try:
                    seq = int(m_seq.group(1))
                    send_ack(node, seq)
                except ValueError:
                    logger.warning("RX: bad SEQ in payload, not ACKing")

        # Log to CSV and file
        ts = now_iso()
        try:
            with open(CSV_PATH, "a") as f:
                f.write(f"{ts},{packet_rssi},{noise_rssi},{payload}\n")
        except Exception as e:
            logger.warning(f"Failed to append CSV: {e}")

        logger.info(f"RX payload={payload} packet_rssi={packet_rssi} noise_rssi={noise_rssi}")

        # Print to console for interactive runs
        print(f"[{ts}] payload={payload} packet_rssi={packet_rssi} noise_rssi={noise_rssi}")

        # Sleep briefly to avoid a tight loop when idle
        time.sleep(0.05)


def main() -> None:
    listen()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nReceiver terminated.")
