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
            freq=915,
            addr=self.addr,
            power=22,
            rssi=True,
            air_speed=1200,
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

    def _fetch_latest_csv(self) -> Tuple[Optional[int], Optional[str], Optional[str]]:
        req = urllib.request.Request(CSV_URL, method="GET")
        req.add_header("Accept", "text/csv, */*;q=0.1")
        req.add_header("User-Agent", "storm3_tx/1.1")
        req.add_header("Accept-Encoding", "gzip")
        if self.if_modified_since:
            req.add_header("If-Modified-Since", self.if_modified_since)

        try:
            with urllib.request.urlopen(req, timeout=6) as resp:
                status = resp.status
                data = resp.read()
                if resp.headers.get("Content-Encoding", "") == "gzip":
                    data = gzip.decompress(data)
                last_mod = resp.headers.get("Last-Modified")
                return status, data.decode("utf-8", errors="replace"), last_mod
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return 304, None, None
            self.log_and_print(f"HTTP error fetching CSV: {e.reason}")
            return None, None, None
        except Exception as e:
            self.log_and_print(f"HTTP error fetching CSV: {e}")
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
        """Create packet with checksum using shortened JSON format"""
        if packet_type == 'STORM_DATA':
            packet_data = {
                'seq': seq_num,
                'src_addr': self.addr,
                'dst_addr': self.target_addr,
                'ts': datetime.now().strftime('%H:%M:%S'),  # shortened timestamp
                'T': round(kwargs.get('board_temp_c', 0), 1),  # board_temp_c -> T
                'c_date': kwargs.get('csv_date'),  # csv_date -> c_date
                'c_time': kwargs.get('csv_time'),  # csv_time -> c_time
                'd': kwargs.get('riverstage'),  # riverstage -> d
                'r': kwargs.get('rain_gauge')   # rain_gauge -> r
            }
        elif packet_type == 'ACK':
            packet_data = {
                'seq': seq_num,
                'src_addr': self.addr,
                'dst_addr': self.target_addr,
                'ts': datetime.now().strftime('%H:%M:%S'),  # shortened timestamp
                'ack': True
            }
        else:
            # Fallback
            packet_data = {
                'seq': seq_num,
                'src_addr': self.addr,
                'dst_addr': self.target_addr,
                'ts': datetime.now().strftime('%H:%M:%S')
            }

        # Create JSON and add checksum
        packet_json = json.dumps(packet_data, separators=(',', ':'))
        checksum = hashlib.md5(packet_json.encode()).hexdigest()[:6]  # shortened checksum
        packet_data['checksum'] = checksum

        final_json = json.dumps(packet_data, separators=(',', ':'))
        
        # Log the packet size for monitoring
        self.log_and_print(f"Packet size: {len(final_json)} bytes")
        
        return final_json.encode()

    def verify_packet(self, packet_data):
        """Verify packet checksum"""
        try:
            data = json.loads(packet_data.decode())
            checksum = data.pop('checksum', None)
            if not checksum:
                return False, None

            packet_json = json.dumps(data, separators=(',', ':'))
            expected_checksum = hashlib.md5(packet_json.encode()).hexdigest()[:6]

            if checksum == expected_checksum:
                data['checksum'] = checksum  # Put it back
                return True, data
            else:
                return False, None
        except Exception as e:
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
                time.sleep(0.1)
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
        """Main transmitter loop - fetches Storm3 data and transmits via LoRa"""
        self.log_and_print("Starting transmitter mode - fetching and broadcasting Storm3 data every 15 minutes")

        next_transmission = time.time()  # Start first transmission immediately
        stats_log_interval = 86400  # Log stats every 24 hours
        next_stats_log = time.time() + stats_log_interval

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

                # Schedule the next transmission for 15 minutes from now
                next_transmission += 900  # 15 minutes = 900 seconds

                # 1. Fetch data
                status, text, last_mod = self._fetch_latest_csv()

                if status == 304:
                    self.log_and_print("No new data (304 Not Modified), sleeping.")
                    continue
                if status != 200 or not text:
                    self.log_and_print(f"Failed to fetch data (status: {status}), sleeping.")
                    continue

                # 2. Extract latest row
                row = self._extract_latest_row(text)
                if not row:
                    self.log_and_print("Could not extract data row from CSV, sleeping.")
                    continue

                csv_date, csv_time, riverstage, rain_gauge = row
                self.log_and_print(f"Fetched new data: {csv_date} {csv_time}, River: {riverstage}, Rain: {rain_gauge}")

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

                    if self.send_packet(packet):
                        self.log_and_print(f"TX Seq#{self.seq_number}: Packet sent, waiting for ACK...")
                        self.update_stats('packets_sent')

                        self.ack_received.clear()
                        self.pending_acks[self.seq_number] = retry_count

                        if self.ack_received.wait(timeout=7):
                            if self.seq_number not in self.pending_acks:
                                self.log_and_print(f"TX Seq#{self.seq_number}: ACK received successfully")
                                self.update_stats('acks_received')
                                success = True
                            else:
                                self.log_and_print(f"TX Seq#{self.seq_number}: ACK timeout (internal state mismatch)")
                        else:
                            self.log_and_print(f"TX Seq#{self.seq_number}: ACK timeout")
                    else:
                        self.log_and_print(f"TX Seq#{self.seq_number}: Failed to send packet")

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
                        # Handle STORM_DATA packets (now using shortened format)
                        if 'd' in packet_info and 'r' in packet_info:  # Shortened STORM_DATA packet
                            self.log_and_print(f"RX: STORM_DATA packet Seq#{packet_info['seq']}")
                            self.log_and_print(f"RX: Date: {packet_info.get('c_date')} {packet_info.get('c_time')}")
                            self.log_and_print(f"RX: River: {packet_info.get('d')}, Rain: {packet_info.get('r')}")
                            self.log_and_print(f"RX: Board Temp: {packet_info.get('T')}°C")

                            self.update_stats('packets_received')

                            # Log to CSV
                            try:
                                with open(self.csv_path, "a") as f:
                                    ts = datetime.now().isoformat()
                                    f.write(f"{ts},{packet_info['seq']},{packet_rssi},{noise_rssi},{snr},"
                                            f"{packet_info.get('T', '')}"
                                            f",\"{packet_info.get('c_date', '')}\",\"{packet_info.get('c_time', '')}\""
                                            f",\"{packet_info.get('d', '')}\",\"{packet_info.get('r', '')}\""
                                            f",\"{packet_info.get('ts', '')}\"\n")
                            except Exception as e:
                                self.log_and_print(f"Failed to write to CSV log: {e}")

                            # Send ACK using proven method
                            ack_packet = self.create_packet('ACK', packet_info['seq'])
                            if self.send_packet(ack_packet):
                                self.log_and_print(f"RX: ACK sent for Seq#{packet_info['seq']}")
                                self.update_stats('acks_sent')
                            else:
                                self.log_and_print(f"RX: Failed to send ACK for Seq#{packet_info['seq']}")

                        elif 'ack' in packet_info:  # ACK packet
                            seq_num = packet_info['seq']
                            if seq_num in self.pending_acks:
                                self.log_and_print(f"RX: ACK received for Seq#{seq_num}")
                                self.pending_acks.pop(seq_num)
                                self.ack_received.set()
                            else:
                                self.log_and_print(f"RX: Unexpected ACK for Seq#{seq_num}")

                    else:
                        self.log_and_print(f"RX: Invalid packet received (checksum failure)")
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
