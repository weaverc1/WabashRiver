#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
LoRa application for Storm3 data transmission.
This script can run as a transmitter or a receiver using JSON packet format.

Transmitter mode:
- Fetches the latest data from a Storm3 device's CSV export.
- Transmits the data using LoRa with JSON packets and ACK mechanism.
- Maintains state to avoid re-transmitting old data.

Receiver mode:
- Listens for LoRa packets.
- Sends ACKs for received data packets.
- Logs received data and statistics.

Usage:
  python lora_app.py tx --addr 1
  python lora_app.py rx --addr 2
"""

import sys
import sx126x
import threading
import time
import random
import argparse
import json
import hashlib
import os
import re
import gzip
import urllib.request
import urllib.error
import uuid
import csv
import io
import math
import base64
from datetime import datetime
from typing import Optional, Tuple
from threading import Timer, Lock, Event

# ---------------------------
# CRC-16/MODBUS calculation
# ---------------------------
# Using a pre-calculated table for efficiency, as recommended.
# Source: https://www.digi.com/resources/documentation/digidocs/90001537/references/r_python_crc16_modbus.htm
CRC16_MODBUS_TABLE = (
    0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241,
    0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
    0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40,
    0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
    0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40,
    0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
    0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641,
    0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
    0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240,
    0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
    0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41,
    0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
    0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41,
    0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
    0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640,
    0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
    0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240,
    0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
    0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41,
    0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
    0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41,
    0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
    0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640,
    0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
    0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241,
    0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
    0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40,
    0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
    0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40,
    0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
    0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641,
    0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040,
)

def crc16_modbus(data: bytes) -> str:
    """
    Calculates the CRC-16/MODBUS checksum for a byte string.
    Returns a 4-character uppercase hexadecimal string.
    """
    crc = 0xFFFF  # Initial value for MODBUS
    for byte in data:
        crc = (crc >> 8) ^ CRC16_MODBUS_TABLE[(crc ^ byte) & 0xFF]
    # Return as 4-char hex string (e.g., "7F3C")
    return f"{crc:04X}"

# ---------------------------
# Storm3 CSV configuration
# ---------------------------
SITE_ID    = "Hutsonville"
STORM_HOST = "172.20.20.20"
CSV_URL    = f"http://{STORM_HOST}/data/{SITE_ID}.csv"

# ---------------------------
# Paths & logging
# ---------------------------

BASE_DIR  = os.path.expanduser("~/storm")

LOG_DIR   = os.path.join(BASE_DIR, "logs")
STATE_DIR = os.path.join(BASE_DIR, "state")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

STATE_FILE = os.path.join(STATE_DIR, "state.json")

class LoRaNode:
    def __init__(self, mode, addr, target_addr=None):
        self.mode = mode
        self.addr = addr
        self.target_addr = target_addr if target_addr else (2 if mode == 'tx' else 1)

        self.log_lock = Lock()
        self.log_filename = os.path.join(LOG_DIR, f"lora_{mode}.log")

        # TX-specific state
        if self.mode == 'tx':
            self.seq_number = 1
            self.if_modified_since = None
            self._load_state()
        else: # rx mode
            self.seq_number = 0
            self.csv_path = os.path.join(LOG_DIR, "rx_data.csv")
            if not os.path.exists(self.csv_path):
                try:
                    with open(self.csv_path, "w") as f:
                        f.write("iso_time,seq,p_rssi,n_rssi,snr,board_temp_c,csv_date,csv_time,riverstage,rain_gauge,tx_timestamp\n")
                except Exception as e:
                    self.log_and_print(f"Failed to create CSV log file: {e}")

        self.ack_received = Event()
        self.pending_acks = {}
        self.ser_lock = Lock()
        self.max_pending_acks = 100

        self.stats = {
            'packets_sent': 0,
            'packets_received': 0,
            'acks_sent': 0,
            'acks_received': 0,
            'failed_packets': 0,
            'invalid_packets': 0,
            'start_time': time.time(),
            'rssi_readings': [],
            'snr_readings': [],
            'noise_readings': []
        }
        self.stats_lock = Lock()

        self.node = sx126x.sx126x(
            serial_num="/dev/serial0",
            freq=924,
            addr=self.addr,
            power=22,
            rssi=True,
            air_speed=2400,
            relay=False
        )

        self.log_and_print(f"LoRa App initialized - Mode: {mode}, Address: {addr}, Target: {self.target_addr}")

    def _load_state(self):
        if not os.path.exists(STATE_FILE):
            self.log_and_print("No state file found, starting fresh.")
            return
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
                self.seq_number = int(state.get("seq", 0)) + 1
                self.if_modified_since = state.get("if_modified_since")
                self.log_and_print(f"Loaded state: seq={self.seq_number-1}, if_modified_since={self.if_modified_since}")
        except Exception as e:
            self.log_and_print(f"Failed to load state, starting fresh: {e}")

    def _save_state(self):
        try:
            state = {
                "seq": self.seq_number,
                "if_modified_since": self.if_modified_since
            }
            with open(STATE_FILE, "w") as f:
                json.dump(state, f)
        except Exception as e:
            self.log_and_print(f"Failed to persist state: {e}")

    def _fetch_latest_csv_with_retry(self) -> Tuple[Optional[int], Optional[str], Optional[str]]:
        """
        Fetch CSV with improved reliability:
        - Multiple retry attempts with exponential backoff
        - Longer timeouts for network issues
        - Different retry strategies for different error types
        """
        max_retries = 3
        base_delay = 2  # seconds

        for attempt in range(max_retries + 1):
            try:
                # Increase timeout progressively
                timeout = 6 + (attempt * 4)  # 6, 10, 14, 18 seconds

                req = urllib.request.Request(CSV_URL, method="GET")
                req.add_header("Accept", "text/csv, */*;q=0.1")
                req.add_header("User-Agent", "storm3_tx/1.1")
                req.add_header("Accept-Encoding", "gzip")
                req.add_header("Connection", "close")  # Force connection close

                if self.if_modified_since:
                    req.add_header("If-Modified-Since", self.if_modified_since)

                self.log_and_print(f"CSV fetch attempt {attempt + 1}/{max_retries + 1} (timeout: {timeout}s)")

                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    status = resp.status
                    data = resp.read()
                    if resp.headers.get("Content-Encoding", "") == "gzip":
                        data = gzip.decompress(data)
                    last_mod = resp.headers.get("Last-Modified")

                    self.log_and_print(f"CSV fetch successful on attempt {attempt + 1}")
                    return status, data.decode("utf-8", errors="replace"), last_mod

            except urllib.error.HTTPError as e:
                if e.code == 304:
                    return 304, None, None
                elif e.code in [500, 502, 503, 504]:  # Server errors - retry
                    self.log_and_print(f"HTTP server error {e.code} on attempt {attempt + 1}: {e.reason}")
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        self.log_and_print(f"Retrying in {delay:.1f} seconds...")
                        time.sleep(delay)
                        continue
                else:  # Client errors (4xx) - don't retry
                    self.log_and_print(f"HTTP client error {e.code}: {e.reason}")
                    return None, None, None

            except urllib.error.URLError as e:
                error_msg = str(e.reason)
                self.log_and_print(f"URL error on attempt {attempt + 1}: {error_msg}")

                # Check for specific network issues
                if any(keyword in error_msg.lower() for keyword in
                       ['network is unreachable', 'no route to host', 'connection refused']):
                    # Network infrastructure issues - longer delay
                    if attempt < max_retries:
                        delay = base_delay * (3 ** attempt) + random.uniform(0, 2)  # More aggressive backoff
                        self.log_and_print(f"Network issue detected, retrying in {delay:.1f} seconds...")
                        time.sleep(delay)
                        continue
                elif 'timed out' in error_msg.lower():
                    # Timeout issues - moderate delay
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        self.log_and_print(f"Timeout detected, retrying in {delay:.1f} seconds...")
                        time.sleep(delay)
                        continue
                else:
                    # Other URL errors - short delay
                    if attempt < max_retries:
                        delay = base_delay + random.uniform(0, 1)
                        self.log_and_print(f"Retrying in {delay:.1f} seconds...")
                        time.sleep(delay)
                        continue

            except Exception as e:
                self.log_and_print(f"Unexpected error on attempt {attempt + 1}: {e}")
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    self.log_and_print(f"Retrying in {delay:.1f} seconds...")
                    time.sleep(delay)
                    continue

        self.log_and_print(f"CSV fetch failed after {max_retries + 1} attempts")
        return None, None, None


    def _fetch_latest_csv_with_fallback(self) -> Tuple[Optional[int], Optional[str], Optional[str]]:
        """
        Enhanced CSV fetching with fallback mechanisms
        """
        # Primary method with retries
        result = self._fetch_latest_csv_with_retry()
        if result[0] is not None:  # Success or 304
            return result

        # Fallback: Try different approach if available
        # Could add alternative endpoints, different protocols, etc.
        self.log_and_print("Primary CSV fetch failed, trying fallback methods...")

        # Example: Try without compression
        try:
            req = urllib.request.Request(CSV_URL, method="GET")
            req.add_header("Accept", "text/csv")
            req.add_header("User-Agent", "storm3_tx/1.1")
            req.add_header("Connection", "close")
            # Don't request gzip compression

            if self.if_modified_since:
                req.add_header("If-Modified-Since", self.if_modified_since)

            self.log_and_print("Fallback: Trying without compression...")

            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                data = resp.read()
                last_mod = resp.headers.get("Last-Modified")

                self.log_and_print("Fallback CSV fetch successful")
                return status, data.decode("utf-8", errors="replace"), last_mod

        except Exception as e:
            self.log_and_print(f"Fallback CSV fetch also failed: {e}")

        return None, None, None

    def _extract_latest_row(self, csv_text: str) -> Optional[Tuple[str, str, str, str]]:
        if not csv_text:
            return None
        lines = [ln.strip() for ln in csv_text.splitlines() if ln.strip()]
        if len(lines) < 2:
            return None
        for ln in reversed(lines):
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) >= 4 and parts[0] and parts[1]:
                date, tm, river, rain = parts[0], parts[1], parts[2], parts[3]
                return date, tm, river, rain
        return None

    def get_cpu_temp(self):
        """Get CPU temperature"""
        try:
            with open("/sys/class/thermal/thermal_zone0/temp", 'r') as temp_file:
                cpu_temp = temp_file.read()
                return float(cpu_temp) / 1000
        except Exception as e:
            self.log_and_print(f"Error reading CPU temperature: {e}")
            return 0.0

    def log_and_print(self, message):
        """Log message to file and print to console"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] {message}"

        print(log_entry)

        with self.log_lock:
            try:
                with open(self.log_filename, 'a', encoding='utf-8') as f:
                    f.write(log_entry + '\n')
                    f.flush()
            except Exception as e:
                print(f"Error writing to log file: {e}")

    def log_statistics(self):
        """Log current statistics"""
        with self.stats_lock:
            runtime = time.time() - self.stats['start_time']
            runtime_hours = runtime / 3600

            # Calculate averages
            avg_rssi = sum(self.stats['rssi_readings']) / len(self.stats['rssi_readings']) if self.stats['rssi_readings'] else 0
            avg_snr = sum(self.stats['snr_readings']) / len(self.stats['snr_readings']) if self.stats['snr_readings'] else 0
            avg_noise = sum(self.stats['noise_readings']) / len(self.stats['noise_readings']) if self.stats['noise_readings'] else 0

            success_rate = 0
            if self.mode == 'tx' and self.stats['packets_sent'] > 0:
                success_rate = (self.stats['acks_received'] / self.stats['packets_sent']) * 100
            elif self.mode == 'rx' and self.stats['packets_received'] > 0:
                success_rate = (self.stats['acks_sent'] / self.stats['packets_received']) * 100

            self.log_and_print(f"STATS: Runtime={runtime_hours:.2f}h, Sent={self.stats['packets_sent']}, "
                             f"Received={self.stats['packets_received']}, ACKs_sent={self.stats['acks_sent']}, "
                             f"ACKs_received={self.stats['acks_received']}, Failed={self.stats['failed_packets']}, "
                             f"Invalid={self.stats['invalid_packets']}, Success_rate={success_rate:.1f}%")

            if self.stats['rssi_readings']:
                self.log_and_print(f"STATS: Avg_RSSI={avg_rssi:.1f}dBm, Avg_SNR={avg_snr:.1f}dB, "
                                 f"Avg_Noise={avg_noise:.1f}dBm, RSSI_samples={len(self.stats['rssi_readings'])}")

    def update_stats(self, stat_type, value=None):
        """Update statistics thread-safely"""
        with self.stats_lock:
            if stat_type in self.stats:
                if value is not None:
                    if stat_type.endswith('_readings'):
                        self.stats[stat_type].append(value)
                        # Keep only last 1000 readings to prevent memory growth
                        if len(self.stats[stat_type]) > 1000:
                            self.stats[stat_type] = self.stats[stat_type][-1000:]
                    else:
                        self.stats[stat_type] = value
                else:
                    self.stats[stat_type] += 1

    def create_packet(self, packet_type: str, seq_num: int, **kwargs):
        """Create packet with checksum using the new shortened JSON format"""
        if packet_type == 'STORM_DATA':
            # Combine date and time into the compact "YYMMDDHHmm" format
            csv_date_str = kwargs.get('csv_date', '')  # "MM/DD/YYYY"
            csv_time_str = kwargs.get('csv_time', '')  # "HH:MM:SS"
            compact_ts = ""
            try:
                # Parse "MM/DD/YYYY HH:MM:SS"
                dt_obj = datetime.strptime(f"{csv_date_str} {csv_time_str}", "%m/%d/%Y %H:%M:%S")
                compact_ts = dt_obj.strftime("%y%m%d%H%M")
            except ValueError:
                self.log_and_print(f"Could not parse date/time: {csv_date_str} {csv_time_str}")

            # Safely convert river and rain to floats, with rounding
            try:
                river_val = round(float(kwargs.get('riverstage', '0')), 3)
            except (ValueError, TypeError):
                river_val = 0.0
            try:
                rain_val = round(float(kwargs.get('rain_gauge', '0')), 2)
            except (ValueError, TypeError):
                rain_val = 0.0

            packet_data = {
                's': seq_num,
                't': compact_ts,
                'd': river_val,
                'r': rain_val,
                'T': round(kwargs.get('board_temp_c', 0), 1)
            }
        elif packet_type == 'ACK':
            packet_data = {
                's': seq_num,
                'a': 1 # 'ack' -> 'a' for short
            }
        else: # Fallback for unknown types
            packet_data = {'s': seq_num}

        # Create JSON and add CRC-16 checksum
        packet_json = json.dumps(packet_data, separators=(',', ':'))
        checksum = crc16_modbus(packet_json.encode('utf-8'))
        packet_data['k'] = checksum # 'checksum' -> 'k'

        final_json = json.dumps(packet_data, separators=(',', ':'))

        # Log the packet size for monitoring
        self.log_and_print(f"Packet size: {len(final_json)} bytes")

        return final_json.encode()

    def verify_packet(self, packet_data):
        """Verify packet CRC-16 checksum"""
        try:
            # It's crucial to decode using 'utf-8' to match the sender's encoding
            data = json.loads(packet_data.decode('utf-8'))
            checksum = data.pop('k', None) # 'checksum' -> 'k'
            if not checksum:
                self.log_and_print("RX: Packet failed verification: missing checksum 'k'")
                return False, None

            # Re-create the JSON string without the checksum to validate
            packet_json = json.dumps(data, separators=(',', ':'))
            expected_checksum = crc16_modbus(packet_json.encode('utf-8'))

            if checksum == expected_checksum:
                data['k'] = checksum  # Add it back for logging/forwarding if needed
                return True, data
            else:
                self.log_and_print(f"RX: Packet failed verification: checksum mismatch. Got: {checksum}, Exp: {expected_checksum}")
                return False, None
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.log_and_print(f"RX: Packet failed verification: could not decode JSON. Error: {e}")
            return False, None
        except Exception as e:
            self.log_and_print(f"RX: An unexpected error occurred during packet verification: {e}")
            return False, None

    def send_packet(self, packet_data):
        """Send packet via LoRa using the proven method from test script"""
        try:
            with self.ser_lock:
                # Create LoRa frame exactly like the working test script
                lora_frame = bytes([
                    self.target_addr >> 8,      # Target address high byte
                    self.target_addr & 0xff,    # Target address low byte  
                    self.node.offset_freq,      # Target frequency offset
                    self.addr >> 8,             # Source address high byte
                    self.addr & 0xff,           # Source address low byte
                    self.node.offset_freq,      # Source frequency offset
                ]) + packet_data

                # Clear any pending data before sending
                self.node.ser.flushInput()
                self.node.ser.flushOutput()
                time.sleep(0.1)  # Allow module to settle

                self.node.send(lora_frame)

                # Small delay after sending to ensure transmission completes
                time.sleep(0.7)
                return True
        except Exception as e:
            self.log_and_print(f"Error sending packet: {e}")
            return False

    def cleanup_pending_acks(self):
        """Remove old pending ACKs to prevent memory growth"""
        if len(self.pending_acks) > self.max_pending_acks:
            # Remove oldest entries (lowest sequence numbers)
            sorted_keys = sorted(self.pending_acks.keys())
            for key in sorted_keys[:len(self.pending_acks) - self.max_pending_acks + 20]:
                self.pending_acks.pop(key, None)
            self.log_and_print(f"TX: Cleaned up old pending ACKs, now tracking {len(self.pending_acks)} entries")

    def transmitter_loop(self):
        """
        Improved transmitter loop with better error handling and adaptive timing
        """
        self.log_and_print("Starting transmitter mode - fetching and broadcasting Storm3 data every 15 minutes")

        next_transmission = time.time()
        stats_log_interval = 86400
        next_stats_log = time.time() + stats_log_interval
        consecutive_failures = 0
        max_consecutive_failures = 3

        while True:
            try:
                # Log statistics periodically
                if time.time() >= next_stats_log:
                    self.log_statistics()
                    next_stats_log = time.time() + stats_log_interval

                # Wait until it's time for the next transmission
                sleep_time = next_transmission - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)

                # Adaptive scheduling: increase interval after consecutive failures
                base_interval = 900  # 15 minutes
                if consecutive_failures >= max_consecutive_failures:
                    # Back off to 30 minutes after multiple failures
                    interval = base_interval * 2
                    self.log_and_print(f"Using extended interval ({interval//60} min) due to consecutive failures")
                else:
                    interval = base_interval

                next_transmission += interval

                # Fetch data with improved reliability
                status, text, last_mod = self._fetch_latest_csv_with_fallback()

                if status == 304:
                    self.log_and_print("No new data (304 Not Modified), sleeping.")
                    consecutive_failures = 0  # Reset counter on successful connection
                    continue

                if status != 200 or not text:
                    consecutive_failures += 1
                    self.log_and_print(f"Failed to fetch data (status: {status}), consecutive failures: {consecutive_failures}")

                    # Early retry for first few failures
                    if consecutive_failures <= 2:
                        self.log_and_print("Scheduling early retry in 5 minutes...")
                        next_transmission = time.time() + 300  # Retry in 5 minutes
                    continue

                # Success - reset failure counter
                consecutive_failures = 0

                # Continue with existing packet transmission logic...
                row = self._extract_latest_row(text)
                if not row:
                    self.log_and_print("Could not extract data row from CSV, sleeping.")
                    continue

                csv_date, csv_time, riverstage, rain_gauge = row
                self.log_and_print(f"Fetched new data: {csv_date} {csv_time}, River: {riverstage}, Rain: {rain_gauge}")

                # [Rest of transmission logic remains the same]
                # 3. Build and send packet
                self.cleanup_pending_acks()

                packet = self.create_packet(
                    'STORM_DATA',
                    self.seq_number,
                    board_temp_c=self.get_cpu_temp(),
                    csv_date=csv_date,
                    csv_time=csv_time,
                    riverstage=riverstage,
                    rain_gauge=rain_gauge
                )

                self.log_and_print(f"TX Seq#{self.seq_number}: Sending Storm3 data packet.")

                success = False
                retry_count = 0
                max_retries = 3

                while retry_count < max_retries and not success:
                    if retry_count > 0:
                        self.log_and_print(f"TX Seq#{self.seq_number}: Retry {retry_count}/{max_retries-1}")

                    # Send the packet twice, 300ms apart
                    self.log_and_print(f"TX Seq#{self.seq_number}: Sending packet 1 of 2.")
                    send1_ok = self.send_packet(packet)
                    time.sleep(0.3)
                    self.log_and_print(f"TX Seq#{self.seq_number}: Sending packet 2 of 2.")
                    send2_ok = self.send_packet(packet)

                    # Consider the transmission attempt successful if at least one packet went out.
                    if send1_ok or send2_ok:
                        self.log_and_print(f"TX Seq#{self.seq_number}: Packet sent (send1: {'OK' if send1_ok else 'FAIL'}, send2: {'OK' if send2_ok else 'FAIL'}), waiting for ACK...")
                        self.update_stats('packets_sent')

                        self.ack_received.clear()
                        self.pending_acks[self.seq_number] = retry_count

                        if self.ack_received.wait(timeout=15):
                            if self.seq_number not in self.pending_acks:
                                self.log_and_print(f"TX Seq#{self.seq_number}: ACK received successfully")
                                self.update_stats('acks_received')
                                success = True
                            else:
                                self.log_and_print(f"TX Seq#{self.seq_number}: ACK timeout (internal state mismatch)")
                        else:
                            self.log_and_print(f"TX Seq#{self.seq_number}: ACK timeout")
                    else:
                        self.log_and_print(f"TX Seq#{self.seq_number}: Failed to send both packets")

                    retry_count += 1
                    if not success and retry_count < max_retries:
                        time.sleep(3)

                # 4. Update state
                if success:
                    self.log_and_print(f"TX Seq#{self.seq_number}: Successfully transmitted and ACKed.")
                    if last_mod:
                        self.if_modified_since = last_mod
                else:
                    self.log_and_print(f"TX Seq#{self.seq_number}: FAILED - No ACK after {max_retries} attempts")
                    self.pending_acks.pop(self.seq_number, None)
                    self.update_stats('failed_packets')

                # Always advance sequence number and save state after an attempt
                self.seq_number += 1
                self._save_state()

            except KeyboardInterrupt:
                self.log_and_print("Transmitter stopped by user")
                self.log_statistics()
                break
            except Exception as e:
                self.log_and_print(f"Transmitter error: {e}")
                consecutive_failures += 1
                time.sleep(5)

    def receiver_loop(self):
        """Main receiver loop using proven method from test script"""
        self.log_and_print("Starting receiver mode - listening for JSON packets")

        stats_log_interval = 86400  # Log stats every 24 hours
        next_stats_log = time.time() + stats_log_interval

        while True:
            try:
                # Log statistics periodically
                if time.time() >= next_stats_log:
                    self.log_statistics()
                    next_stats_log = time.time() + stats_log_interval

                # Check for incoming packets (same method as working test script)
                with self.ser_lock:
                    if self.node.ser.inWaiting() > 0:
                        time.sleep(0.2)  # Allow full packet to arrive and settle
                        bytes_available = self.node.ser.inWaiting()
                        if bytes_available > 0:
                            raw_data = self.node.ser.read(bytes_available)
                        else:
                            continue
                    else:
                        time.sleep(0.2)
                        continue

                if len(raw_data) > 3:  # Minimum frame size
                    # Parse LoRa frame as received: [src_addr_h, src_addr_l, src_freq, payload..., rssi]
                    src_addr = (raw_data[0] << 8) + raw_data[1]
                    src_freq = raw_data[2]
                    packet_data = raw_data[3:-1] if self.node.rssi else raw_data[3:]
                    rssi_value = raw_data[-1] if self.node.rssi else None

                    # Validate RSSI value
                    valid_rssi = True
                    if rssi_value is not None:
                        calculated_rssi = -(256 - rssi_value)
                        if calculated_rssi < -120 or calculated_rssi > -30:
                            valid_rssi = False
                            self.log_and_print(f"RX: Dropping packet with invalid RSSI: {calculated_rssi}dBm")

                    # Accept packets from our target (the TX unit)
                    if src_addr != self.target_addr:
                        continue

                    # Skip corrupted packets
                    if not valid_rssi:
                        self.update_stats('invalid_packets')
                        continue

                    # Calculate RSSI values
                    packet_rssi = None
                    noise_rssi = None
                    snr = None

                    if rssi_value is not None:
                        packet_rssi = -(256 - rssi_value)
                        self.update_stats('rssi_readings', packet_rssi)

                        # Get noise floor
                        for attempt in range(3):
                            noise_rssi = self.get_noise_rssi()
                            if noise_rssi is not None and noise_rssi >= -120 and noise_rssi <= -30:
                                break
                            time.sleep(0.05)

                        if noise_rssi is not None and packet_rssi is not None:
                            snr = packet_rssi - noise_rssi
                            self.update_stats('snr_readings', snr)
                            self.update_stats('noise_readings', noise_rssi)

                    # Log reception details
                    freq_mhz = src_freq + self.node.start_freq
                    self.log_and_print(f"RX: Received from addr {src_addr} at {freq_mhz}.125MHz")

                    if packet_rssi is not None:
                        noise_str = f"{noise_rssi}dBm" if noise_rssi is not None else "N/A"
                        snr_str = f"{snr:.1f}dB" if snr is not None else "N/A"
                        self.log_and_print(f"RX: Packet RSSI: {packet_rssi}dBm, Noise: {noise_str}, SNR: {snr_str}")

                    # Verify and process packet
                    valid, packet_info = self.verify_packet(packet_data)

                    if valid:
                        seq_num = packet_info.get('s')
                        if seq_num is None:
                            self.log_and_print("RX: Invalid packet, missing sequence number 's'")
                            self.update_stats('invalid_packets')
                            continue

                        # Handle STORM_DATA packets (new shortened format)
                        if 'd' in packet_info and 'r' in packet_info:
                            self.log_and_print(f"RX: STORM_DATA packet Seq#{seq_num}")

                            # Reconstruct date/time for logging from compact format
                            compact_ts = packet_info.get('t', '')
                            csv_date, csv_time = "", ""
                            if compact_ts:
                                try:
                                    dt_obj = datetime.strptime(compact_ts, "%y%m%d%H%M")
                                    csv_date = dt_obj.strftime("%m/%d/%Y")
                                    csv_time = dt_obj.strftime("%H:%M:%S")
                                except ValueError:
                                    self.log_and_print(f"RX: Could not parse compact timestamp '{compact_ts}'")

                            self.log_and_print(f"RX: Timestamp: {csv_date} {csv_time}")
                            self.log_and_print(f"RX: River: {packet_info.get('d')}, Rain: {packet_info.get('r')}")
                            self.log_and_print(f"RX: Board Temp: {packet_info.get('T')}°C")

                            self.update_stats('packets_received')

                            # Log to CSV, maintaining the original format
                            try:
                                with open(self.csv_path, "a") as f:
                                    ts = datetime.now().isoformat()
                                    # The old 'ts' field (tx_timestamp) from the packet is gone, log an empty string
                                    f.write(f"{ts},{seq_num},{packet_rssi},{noise_rssi},{snr},"
                                            f"{packet_info.get('T', '')},"
                                            f"\"{csv_date}\",\"{csv_time}\","
                                            f"\"{packet_info.get('d', '')}\",\"{packet_info.get('r', '')}\",\"\"\n")
                            except Exception as e:
                                self.log_and_print(f"Failed to write to CSV log: {e}")

                            # Send ACK
                            ack_packet = self.create_packet('ACK', seq_num)
                            if self.send_packet(ack_packet):
                                self.log_and_print(f"RX: ACK sent for Seq#{seq_num}")
                                self.update_stats('acks_sent')
                            else:
                                self.log_and_print(f"RX: Failed to send ACK for Seq#{seq_num}")

                        # Handle ACK packets (new shortened format)
                        elif 'a' in packet_info:
                            if self.mode == 'tx':  # Transmitter processes ACKs
                                if seq_num in self.pending_acks:
                                    self.log_and_print(f"RX: ACK received for Seq#{seq_num}")
                                    self.pending_acks.pop(seq_num)
                                    self.ack_received.set()
                                else:
                                    self.log_and_print(f"RX: Unexpected or late ACK for Seq#{seq_num}")
                            else:  # Receiver got an ACK, which is unusual
                                self.log_and_print(f"RX: Received an ACK packet (Seq#{seq_num}), but in RX-only mode.")

                    else:
                        self.log_and_print(f"RX: Invalid packet received (checksum failure or malformed)")
                        self.update_stats('invalid_packets')

            except KeyboardInterrupt:
                self.log_and_print("Receiver stopped by user")
                self.log_statistics()
                break
            except Exception as e:
                self.log_and_print(f"Receiver error: {e}")
                time.sleep(1)

    def get_noise_rssi(self):
        """Get current noise floor RSSI"""
        try:
            self.node.ser.flushInput()
            self.node.ser.flushOutput()
            self.node.ser.write(bytes([0xC0, 0xC1, 0xC2, 0xC3, 0x00, 0x02]))

            timeout = time.time() + 0.5
            while time.time() < timeout:
                if self.node.ser.inWaiting() >= 4:
                    break
                time.sleep(0.01)

            if self.node.ser.inWaiting() >= 4:
                response = self.node.ser.read(4)
                if len(response) >= 4 and response[0] == 0xC1 and response[1] == 0x00 and response[2] == 0x02:
                    noise_rssi = -(256 - response[3])
                    if -150 <= noise_rssi <= -30:
                        return noise_rssi
            return None
        except Exception as e:
            self.log_and_print(f"Error getting noise RSSI: {e}")
            return None

    def run(self):
        """Run the application based on mode"""
        try:
            if self.mode == 'tx':
                # Start receiver thread for ACKs
                rx_thread = threading.Thread(target=self.receiver_loop, daemon=True)
                rx_thread.start()
                # Run transmitter in main thread
                self.transmitter_loop()
            else:
                # Run receiver only
                self.receiver_loop()
        except Exception as e:
            self.log_and_print(f"Fatal error: {e}")
        finally:
            self.log_and_print(f"Application completed. Log saved to: {self.log_filename}")

def main():
    parser = argparse.ArgumentParser(description='LoRa app for Storm3 data transmission')
    parser.add_argument('mode', choices=['tx', 'rx'], help='Mode: tx (transmitter) or rx (receiver)')
    parser.add_argument('--addr', type=int, required=True, help='This device address')
    parser.add_argument('--target', type=int, help='Target device address (optional, auto-determined)')

    args = parser.parse_args()

    # Create and run application
    app = LoRaNode(
        mode=args.mode,
        addr=args.addr,
        target_addr=args.target
    )

    try:
        app.run()
    except KeyboardInterrupt:
        print("\nApp interrupted by user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
