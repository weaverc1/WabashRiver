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
from contextlib import redirect_stdout

# Add project root to allow importing 'wabash'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from wabash import common

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MY_ADDR   = common.ADDR_RX
PEER_ADDR = common.ADDR_TX

BASE_DIR = os.path.expanduser("~/storm/receiver")
LOG_DIR  = os.path.join(BASE_DIR, "logs")
CSV_PATH = os.path.join(LOG_DIR, "rx.csv")

# Set up logging with rotation
logger = common.setup_logging("storm3_rx", BASE_DIR, max_bytes=200_000)

# If the CSV log does not exist, write the header
if not os.path.exists(CSV_PATH):
    with open(CSV_PATH, "w") as f:
        f.write("iso_time,packet_rssi,noise_rssi,payload\n")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def now_iso() -> str:
    """Return the current time in ISO-8601 format with UTC suffix."""
    return datetime.utcnow().isoformat() + "Z"


def send_ack(node, seq: int) -> None:
    """Send TYPE=ACK;SEQ=<n> back to the transmitter."""
    ack_payload = f"TYPE=ACK;SEQ={seq}".encode()
    hdr = common.build_header(PEER_ADDR, MY_ADDR)
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
    node = common.init_radio(my_addr=MY_ADDR, for_tx=False)
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
