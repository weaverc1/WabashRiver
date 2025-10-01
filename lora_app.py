#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
LoRa application for Storm3 data transmission.
This script can run as a transmitter or a receiver using JSON packet format.

Transmitter mode:
- Fetches the latest data from a Storm3 device's CSV export.
- Calibrates clock offset between Pi and Storm3 for optimal timing.
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
import socket
from datetime import datetime
from typing import Optional, Tuple
from threading import Timer, Lock, Event

# ---------------------------
# CRC-16/MODBUS calculation
# ---------------------------
CRC16_MODBUS_TABLE = [
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
    0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040
]

def crc16_modbus(data: bytes) -> str:
    """
    Compute CRC-16/MODBUS on 'data' (a bytes object).
    Returns a 4-character uppercase hexadecimal string.
    """
    crc = 0xFFFF
    for byte in data:
        crc = (crc >> 8) ^ CRC16_MODBUS_TABLE[(crc ^ byte) & 0xFF]
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

def check_storm3_tcp_alive(timeout=2):
    """Quick TCP check to verify Storm3 is responsive before HTTP attempt"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((STORM_HOST, 80))
        sock.close()
        return True
    except:
        return False

def parse_csv_timestamp(csv_date, csv_time):
    """Convert CSV date/time to Unix timestamp"""
    try:
        dt_str = f"{csv_date} {csv_time}"
        dt = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
        return dt.timestamp()
    except:
        return None

class LoRaNode:
    def __init__(self, mode, addr, target_addr=None):
        self.mode = mode
        self.addr = addr
        self.target_addr = target_addr if target_addr else (2 if mode == 'tx' else 1)

        self.log_lock = Lock()
        self.log_filename = os.path.join(LOG_DIR, f"lora_{mode}.log")
        
        # Ensure log file exists with proper permissions
        try:
            if not os.path.exists(self.log_filename):
                with open(self.log_filename, 'w') as f:
                    f.write(f"# LoRa {mode.upper()} Log - Started {datetime.now().isoformat()}\n")
            print(f"Logging to: {self.log_filename}")
        except Exception as e:
            print(f"Warning: Could not create log file {self.log_filename}: {e}")

        # TX-specific state
        if self.mode == 'tx':
            self.seq_number = 1
            self.if_modified_since = None
            self._load_state()
            
            # Clock calibration tracking
            self.clock_offset = None
            self.last_storm3_timestamp = None
            self.last_pi_timestamp = None
            self.calibration_confidence = 0
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
            freq=909,
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
        """Simplified fetch: one long attempt with minimal headers"""
        try:
            req = urllib.request.Request(CSV_URL, method="GET")
            req.add_header("User-Agent", "Mozilla/5.0")
            
            self.log_and_print("Fetching CSV (timeout: 40s)")
            
            with urllib.request.urlopen(req, timeout=40) as resp:
                status = resp.status
                data = resp.read()
                last_mod = resp.headers.get("Last-Modified")
                
                self.log_and_print("CSV fetch successful")
                return status, data.decode("utf-8", errors="replace"), last_mod
                
        except Exception as e:
            self.log_and_print(f"CSV fetch failed: {e}")
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
        """Thread-safe logging to file and console"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] {message}"
        
        with self.log_lock:
            print(log_entry)
            try:
                with open(self.log_filename, 'a') as f:
                    f.write(log_entry + '\n')
            except Exception as e:
                print(f"Failed to write to log file: {e}")

    def update_stats(self, stat_name, value=None):
        """Thread-safe statistics update"""
        with self.stats_lock:
            if stat_name in ['rssi_readings', 'snr_readings', 'noise_readings']:
                if value is not None:
                    self.stats[stat_name].append(value)
            else:
                self.stats[stat_name] = self.stats.get(stat_name, 0) + 1

    def log_statistics(self):
        """Log current statistics"""
        with self.stats_lock:
            runtime = time.time() - self.stats['start_time']
            hours = runtime / 3600
            
            self.log_and_print("=== Statistics ===")
            self.log_and_print(f"Runtime: {hours:.2f} hours")
            self.log_and_print(f"Packets sent: {self.stats['packets_sent']}")
            self.log_and_print(f"Packets received: {self.stats['packets_received']}")
            self.log_and_print(f"ACKs sent: {self.stats['acks_sent']}")
            self.log_and_print(f"ACKs received: {self.stats['acks_received']}")
            self.log_and_print(f"Failed packets: {self.stats['failed_packets']}")
            self.log_and_print(f"Invalid packets: {self.stats['invalid_packets']}")
            
            if self.stats['rssi_readings']:
                avg_rssi = sum(self.stats['rssi_readings']) / len(self.stats['rssi_readings'])
                self.log_and_print(f"Average RSSI: {avg_rssi:.1f} dBm")
            
            if self.stats['snr_readings']:
                avg_snr = sum(self.stats['snr_readings']) / len(self.stats['snr_readings'])
                self.log_and_print(f"Average SNR: {avg_snr:.1f} dB")

    def calibrate_clocks(self, storm3_timestamp):
        """Calibrate Pi clock relative to Storm3 clock"""
        pi_time = time.time()
        
        if self.last_storm3_timestamp is None:
            self.last_storm3_timestamp = storm3_timestamp
            self.last_pi_timestamp = pi_time
            self.log_and_print("Clock calibration: first data point recorded")
            return False
        
        storm3_elapsed = storm3_timestamp - self.last_storm3_timestamp
        pi_elapsed = pi_time - self.last_pi_timestamp
        
        if 800 < storm3_elapsed < 1000:
            self.clock_offset = storm3_timestamp - pi_time
            self.calibration_confidence = min(self.calibration_confidence + 1, 10)
            
            offset_min = self.clock_offset / 60
            self.log_and_print(f"Clock calibration updated: Storm3 is {offset_min:+.1f} min relative to Pi")
            self.log_and_print(f"Storm3 elapsed: {storm3_elapsed:.1f}s, Pi elapsed: {pi_elapsed:.1f}s")
            self.log_and_print(f"Calibration confidence: {self.calibration_confidence}/10")
            
            self.last_storm3_timestamp = storm3_timestamp
            self.last_pi_timestamp = pi_time
            return True
        else:
            self.log_and_print(f"Unexpected Storm3 time gap: {storm3_elapsed:.1f}s, resetting calibration")
            self.last_storm3_timestamp = storm3_timestamp
            self.last_pi_timestamp = pi_time
            self.calibration_confidence = 0
            return False
    
    def calculate_next_fetch_time(self, current_storm3_timestamp):
        """Calculate when to fetch next, accounting for clock offset"""
        if self.clock_offset is None or self.calibration_confidence < 2:
            # Not calibrated yet - use Storm3's 15-minute interval
            next_fetch = time.time() + (15 * 60)
            self.log_and_print("Not calibrated, using 15-minute interval")
            return next_fetch
        
        # Predict Storm3's next write time
        next_storm3_write = current_storm3_timestamp + 900
        
        # Convert to Pi time and add 3-minute buffer
        next_fetch_pi_time = next_storm3_write - self.clock_offset + 180
        
        # Sanity check: shouldn't be more than 20 minutes away
        time_until_fetch = next_fetch_pi_time - time.time()
        if 0 < time_until_fetch < 1200:
            next_storm3_dt = datetime.fromtimestamp(next_storm3_write)
            next_storm3_str = next_storm3_dt.strftime('%H:%M:%S')
            self.log_and_print(f"Calibrated timing: Storm3 writes at {next_storm3_str}, fetching 3min after")
            return next_fetch_pi_time
        else:
            # Calibration seems off - fall back to 15-minute interval
            self.log_and_print(f"Calibration suspect (fetch in {time_until_fetch/60:.1f}min), using 15-minute interval")
            self.calibration_confidence = 0
            return time.time() + (15 * 60)

    def create_packet(self, packet_type, seq_num, **kwargs):
        """Create a JSON packet with CRC-16/MODBUS checksum"""
        # Compact timestamp format: YYMMDDHHMM (10 chars vs 19)
        if 'csv_date' in kwargs and 'csv_time' in kwargs:
            try:
                dt = datetime.strptime(f"{kwargs['csv_date']} {kwargs['csv_time']}", "%m/%d/%Y %H:%M:%S")
                compact_ts = dt.strftime("%y%m%d%H%M")
            except:
                compact_ts = ""
        else:
            compact_ts = ""
        
        payload = {
            'type': packet_type,   
            's': seq_num,          
            'T': kwargs.get('board_temp_c', 0.0),
        }
        
        # Add timestamp if available (for both SYSTEM and STORM_DATA)
        if compact_ts:
            payload['t'] = compact_ts
        
        # Add data fields for STORM_DATA packets
        if packet_type == 'STORM_DATA':
            payload.update({
                'd': kwargs.get('riverstage', ''),
                'r': kwargs.get('rain_gauge', '')   
            })
        
        payload_json = json.dumps(payload, separators=(',', ':'))
        payload_bytes = payload_json.encode('utf-8')
        
        checksum = crc16_modbus(payload_bytes)
        
        # Add checksum - the }} in f-string becomes single } in output
        packet_with_checksum = payload_json[:-1] + f',"c":"{checksum}"}}'
        
        return packet_with_checksum.encode('utf-8')

    def verify_packet(self, packet_data):
        """Verify packet integrity using CRC-16/MODBUS checksum"""
        try:
            # Strategy: decode with replacement to check for garbage bytes
            packet_str = packet_data.decode('utf-8', errors='replace')
            
            # Check if there are replacement characters (garbage bytes)
            if '�' in packet_str:
                # Find the last } before any replacement characters
                last_brace = packet_data.rfind(b'}')
                if last_brace == -1:
                    self.log_and_print("RX: No closing brace found in packet")
                    return False, None
                
                # Trim to just the JSON part
                packet_data = packet_data[:last_brace + 1]
                packet_str = packet_data.decode('utf-8', errors='strict')
                
                self.log_and_print(f"DEBUG RX: Trimmed garbage bytes, len={len(packet_str)}")
            
            packet_info = json.loads(packet_str)
            
            if 'c' not in packet_info:
                self.log_and_print("RX: Packet missing checksum field 'c'")
                return False, None
            
            received_checksum = packet_info.pop('c')
            
            payload_for_check = json.dumps(packet_info, separators=(',', ':')).encode('utf-8')
            expected_checksum = crc16_modbus(payload_for_check)
            
            if received_checksum != expected_checksum:
                self.log_and_print(f"RX: Checksum mismatch. Got: {received_checksum}, Exp: {expected_checksum}")
                return False, None
            
            return True, packet_info
            
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.log_and_print(f"RX: Packet failed verification: {e}")
            return False, None
        except Exception as e:
            self.log_and_print(f"RX: Verification error: {e}")
            return False, None

    def send_packet(self, packet_data):
        """Send packet via LoRa"""
        try:
            with self.ser_lock:
                lora_frame = bytes([
                    self.target_addr >> 8,
                    self.target_addr & 0xff,
                    self.node.offset_freq,
                    self.addr >> 8,
                    self.addr & 0xff,
                    self.node.offset_freq,
                ]) + packet_data

                self.node.ser.flushInput()
                self.node.ser.flushOutput()
                time.sleep(0.1)

                self.node.send(lora_frame)

                time.sleep(0.7)
                return True
        except Exception as e:
            self.log_and_print(f"Error sending packet: {e}")
            return False

    def cleanup_pending_acks(self):
        """Remove old pending ACKs to prevent memory growth"""
        if len(self.pending_acks) > self.max_pending_acks:
            sorted_keys = sorted(self.pending_acks.keys())
            for key in sorted_keys[:len(self.pending_acks) - self.max_pending_acks + 20]:
                self.pending_acks.pop(key, None)
            self.log_and_print(f"TX: Cleaned up old pending ACKs, now tracking {len(self.pending_acks)} entries")

    def initialize_on_boot(self):
        """Initialization routine on app startup"""
        boot_time = datetime.now()
        self.log_and_print(f"=== BOOT INITIALIZATION at {boot_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
        
        wakeup_packet = self.create_packet(
            'SYSTEM',
            0,
            board_temp_c=self.get_cpu_temp(),
            csv_date=boot_time.strftime("%m/%d/%Y"),
            csv_time=boot_time.strftime("%H:%M:%S"),
            riverstage=0.0,
            rain_gauge=0.0
        )
        
        self.log_and_print("Sending wake-up announcement to receiver...")
        self.send_packet(wakeup_packet)
        time.sleep(0.3)
        self.send_packet(wakeup_packet)
        
        self.log_and_print("Attempting initial Storm3 sync...")
        
        for attempt in range(3):
            if not check_storm3_tcp_alive(timeout=3):
                self.log_and_print(f"Storm3 TCP not responding (attempt {attempt + 1}/3)")
                time.sleep(10)
                continue
            
            status, csv_text, last_mod = self._fetch_latest_csv_with_retry()
            
            if status and csv_text:
                row = self._extract_latest_row(csv_text)
                if row:
                    csv_date, csv_time, riverstage, rain_gauge = row
                    data_timestamp = parse_csv_timestamp(csv_date, csv_time)
                    
                    if data_timestamp:
                        # Start clock calibration with first data point
                        self.calibrate_clocks(data_timestamp)
                        
                        # Schedule first fetch using 15-minute interval (not calibrated yet)
                        next_fetch_time = time.time() + (15 * 60)
                        
                        self.log_and_print(f"Initial sync: last Storm3 data at {csv_date} {csv_time}")
                        next_fetch_dt = datetime.fromtimestamp(next_fetch_time)
                        self.log_and_print(f"Next fetch scheduled for {next_fetch_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                        self.log_and_print("=== INITIALIZATION COMPLETE ===")
                        return next_fetch_time
            
            self.log_and_print(f"Sync attempt {attempt + 1}/3 failed, retrying in 10s...")
            time.sleep(10)
        
        # If all sync attempts failed, schedule fetch for 15 minutes from now
        fallback_time = time.time() + (15 * 60)
        self.log_and_print("Could not sync to Storm3, will try first fetch in 15 minutes")
        self.log_and_print("=== INITIALIZATION COMPLETE (FALLBACK MODE) ===")
        return fallback_time

    def transmitter_loop(self):
        """Transmitter with clock calibration for optimal timing"""
        next_fetch_time = self.initialize_on_boot()
        consecutive_failures = 0
        
        while True:
            try:
                sleep_time = next_fetch_time - time.time()
                if sleep_time > 0:
                    next_fetch_dt = datetime.fromtimestamp(next_fetch_time)
                    self.log_and_print(f"Next fetch at {next_fetch_dt.strftime('%Y-%m-%d %H:%M:%S')} (in {sleep_time:.0f}s)")
                    time.sleep(sleep_time)
                
                if not check_storm3_tcp_alive(timeout=3):
                    self.log_and_print("Storm3 not responding to TCP, will retry in 60s")
                    next_fetch_time = time.time() + 60
                    consecutive_failures += 1
                    continue
                
                status, csv_text, last_mod = self._fetch_latest_csv_with_retry()
                
                if status is None:
                    self.log_and_print("CSV fetch failed, will retry in 60s")
                    next_fetch_time = time.time() + 60
                    consecutive_failures += 1
                    continue
                
                row = self._extract_latest_row(csv_text)
                if not row:
                    self.log_and_print("No valid data in CSV, will retry in 60s")
                    next_fetch_time = time.time() + 60
                    consecutive_failures += 1
                    continue
                
                csv_date, csv_time, riverstage, rain_gauge = row
                self.log_and_print(f"Fetched data: {csv_date} {csv_time}, River: {riverstage}, Rain: {rain_gauge}")
                
                data_timestamp = parse_csv_timestamp(csv_date, csv_time)
                if data_timestamp:
                    self.calibrate_clocks(data_timestamp)
                    next_fetch_time = self.calculate_next_fetch_time(data_timestamp)
                else:
                    # Can't parse timestamp, use 15-minute interval
                    next_fetch_time = time.time() + (15 * 60)
                    self.log_and_print("Could not parse timestamp, using 15-minute interval")
                
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
                
                self.log_and_print(f"TX Seq#{self.seq_number}: Sending Storm3 data packet")
                
                success = False
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries and not success:
                    if retry_count > 0:
                        self.log_and_print(f"TX Seq#{self.seq_number}: Retry {retry_count}/{max_retries-1}")
                    
                    send1_ok = self.send_packet(packet)
                    time.sleep(0.3)
                    send2_ok = self.send_packet(packet)
                    
                    if send1_ok or send2_ok:
                        self.log_and_print(f"TX Seq#{self.seq_number}: Packets sent, waiting for ACK...")
                        self.update_stats('packets_sent')
                        
                        self.ack_received.clear()
                        self.pending_acks[self.seq_number] = retry_count
                        
                        if self.ack_received.wait(timeout=15):
                            if self.seq_number not in self.pending_acks:
                                self.log_and_print(f"TX Seq#{self.seq_number}: ACK received")
                                self.update_stats('acks_received')
                                success = True
                        
                        if not success:
                            self.log_and_print(f"TX Seq#{self.seq_number}: ACK timeout")
                    
                    retry_count += 1
                    if not success and retry_count < max_retries:
                        time.sleep(3)
                
                if success:
                    self.log_and_print(f"TX Seq#{self.seq_number}: Successfully transmitted")
                    if last_mod:
                        self.if_modified_since = last_mod
                    consecutive_failures = 0
                else:
                    self.log_and_print(f"TX Seq#{self.seq_number}: FAILED after {max_retries} attempts")
                    self.pending_acks.pop(self.seq_number, None)
                    self.update_stats('failed_packets')
                    consecutive_failures += 1
                
                self.seq_number += 1
                self._save_state()
                
            except KeyboardInterrupt:
                self.log_and_print("Transmitter stopped by user")
                self.log_statistics()
                break
            except Exception as e:
                self.log_and_print(f"Transmitter error: {e}")
                next_fetch_time = time.time() + 60
                consecutive_failures += 1

    def receiver_loop(self):
        """Main receiver loop"""
        self.log_and_print("Starting receiver mode - listening for JSON packets")

        stats_log_interval = 86400
        next_stats_log = time.time() + stats_log_interval

        while True:
            try:
                if time.time() >= next_stats_log:
                    self.log_statistics()
                    next_stats_log = time.time() + stats_log_interval

                with self.ser_lock:
                    if self.node.ser.inWaiting() > 0:
                        time.sleep(0.2)
                        bytes_available = self.node.ser.inWaiting()
                        if bytes_available > 0:
                            raw_data = self.node.ser.read(bytes_available)
                        else:
                            continue
                    else:
                        time.sleep(0.2)
                        continue

                if len(raw_data) > 3:
                    src_addr = (raw_data[0] << 8) | raw_data[1]
                    src_freq = raw_data[2]
                    packet_data = raw_data[3:]

                    packet_rssi = None
                    noise_rssi = None
                    snr = None
                    
                    try:
                        packet_rssi = self.node.get_rssi()
                        noise_rssi = self.get_noise_rssi()
                        if packet_rssi is not None and noise_rssi is not None:
                            snr = packet_rssi - noise_rssi
                            self.update_stats('rssi_readings', packet_rssi)
                            self.update_stats('snr_readings', snr)
                            self.update_stats('noise_readings', noise_rssi)
                    except:
                        pass

                    freq_mhz = 850 + src_freq
                    self.log_and_print(f"RX: Received from addr {src_addr} at {freq_mhz}.125MHz")

                    if packet_rssi is not None:
                        noise_str = f"{noise_rssi}dBm" if noise_rssi is not None else "N/A"
                        snr_str = f"{snr:.1f}dB" if snr is not None else "N/A"
                        self.log_and_print(f"RX: Packet RSSI: {packet_rssi}dBm, Noise: {noise_str}, SNR: {snr_str}")

                    valid, packet_info = self.verify_packet(packet_data)

                    if valid:
                        seq_num = packet_info.get('s')
                        if seq_num is None:
                            self.log_and_print("RX: Invalid packet, missing sequence number 's'")
                            self.update_stats('invalid_packets')
                            continue

                        packet_type = packet_info.get('type', 'UNKNOWN')
                        
                        if packet_type == 'SYSTEM':
                            self.log_and_print(f"RX: SYSTEM packet Seq#{seq_num} - Transmitter boot announcement")
                            compact_ts = packet_info.get('t', '')
                            if compact_ts:
                                try:
                                    dt_obj = datetime.strptime(compact_ts, "%y%m%d%H%M")
                                    boot_time = dt_obj.strftime("%m/%d/%Y %H:%M:%S")
                                    self.log_and_print(f"RX: Transmitter booted at {boot_time}")
                                except:
                                    pass
                            continue

                        if 'd' in packet_info and 'r' in packet_info:
                            self.log_and_print(f"RX: STORM_DATA packet Seq#{seq_num}")

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

                            ack_packet = self.create_packet('ACK', seq_num, board_temp_c=self.get_cpu_temp())
                            self.log_and_print(f"RX: Sending ACK for Seq#{seq_num}")
                            self.send_packet(ack_packet)
                            self.update_stats('acks_sent')

                            try:
                                with open(self.csv_path, 'a') as csvfile:
                                    writer = csv.writer(csvfile)
                                    writer.writerow([
                                        datetime.now().isoformat(),
                                        seq_num,
                                        packet_rssi if packet_rssi is not None else '',
                                        noise_rssi if noise_rssi is not None else '',
                                        snr if snr is not None else '',
                                        packet_info.get('T', ''),
                                        csv_date,
                                        csv_time,
                                        packet_info.get('d', ''),
                                        packet_info.get('r', ''),
                                        compact_ts
                                    ])
                            except Exception as e:
                                self.log_and_print(f"RX: Failed to write to CSV: {e}")

                        elif packet_type == 'ACK':
                            self.log_and_print(f"RX: ACK received for Seq#{seq_num}")
                            if seq_num in self.pending_acks:
                                self.pending_acks.pop(seq_num)
                                self.ack_received.set()
                        else:
                            self.log_and_print(f"RX: Unknown packet type: {packet_type}")
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
                rx_thread = threading.Thread(target=self.receiver_loop, daemon=True)
                rx_thread.start()
                self.transmitter_loop()
            else:
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
