#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""
LoRa application for acquiring, transmitting and receiving Storm3.
This script can run as a transmitter or a receiver using JSON packet format.

Transmitter mode:
- Fetches the latest data from a Storm3 device's CSV export.
- Transmits the data using LoRa with JSON packets and ACK mechanism.
- Maintains state to avoid re-transmitting old data.
- Handles remote data fetch requests from receivers.

Receiver mode:
- Listens for LoRa packets.
- Sends ACKs for received data packets.
- Logs received data and statistics.
- Can request historical data from transmitters.

Usage:
  python lora_app.py tx --addr 1
  python lora_app.py rx --addr 2
  python lora_app.py rx --addr 2 --fetch-start "2025-09-15T14:00:00" --fetch-end "2025-09-16T15:00:00"
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
    CHUNK_SIZE = 200  # LoRa packet size limit for payload
    MAX_RETRIES = 3
    ACK_TIMEOUT = 10
    FETCH_REQUEST_TIMEOUT = 30
    
    def __init__(self, mode, addr, target_addr=None, fetch_start=None, fetch_end=None):
        self.mode = mode
        self.addr = addr
        self.target_addr = target_addr if target_addr else (2 if mode == 'tx' else 1)
        self.fetch_start = fetch_start
        self.fetch_end = fetch_end
        
        # Transfer management
        self.pending_transfers = {}  # For TX side - outgoing data transfers
        self.pending_downloads = {}  # For RX side - incoming data transfers
        self.transfer_lock = Lock()

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
                        f.write("iso_time,seq,p_rssi,n_rssi,snr,board_temp_c,csv_date,csv_time,riverstage,rain_gauge,site_id,tx_timestamp\n")
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
            'fetch_requests_sent': 0,
            'fetch_requests_handled': 0,
            'data_chunks_sent': 0,
            'data_chunks_received': 0,
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

            self.log_and_print(f"STATS: Fetch_requests_sent={self.stats['fetch_requests_sent']}, "
                             f"Fetch_requests_handled={self.stats['fetch_requests_handled']}, "
                             f"Chunks_sent={self.stats['data_chunks_sent']}, "
                             f"Chunks_received={self.stats['data_chunks_received']}")

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

    def create_packet(self, packet_type: str, seq_num: int, payload: dict = None, request_id: Optional[str] = None):
        """Create packet with checksum using JSON format"""
        packet_data = {
            'type': packet_type,
            'seq': seq_num,
            'src_addr': self.addr,
            'dst_addr': self.target_addr,
            'payload': payload if payload is not None else {},
            'timestamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        }
        
        if request_id:
            packet_data['request_id'] = request_id

        # Create JSON with consistent ordering and add checksum
        packet_json = json.dumps(packet_data, separators=(',', ':'), sort_keys=True)
        checksum = hashlib.md5(packet_json.encode()).hexdigest()[:8]
        packet_data['checksum'] = checksum

        final_packet = json.dumps(packet_data, separators=(',', ':'), sort_keys=True).encode()
        
        # Add packet size logging for debugging
        packet_size = len(final_packet)
        if packet_size > 240:
            self.log_and_print(f"WARNING: Large packet size {packet_size} bytes for type {packet_type}")
        
        return final_packet

    def verify_packet(self, packet_data):
        """Verify packet checksum"""
        try:
            data = json.loads(packet_data.decode())
            received_checksum = data.pop('checksum', None)
            if not received_checksum:
                self.log_and_print("Packet missing checksum field")
                return False, None

            # Calculate checksum with consistent ordering
            packet_json = json.dumps(data, separators=(',', ':'), sort_keys=True)
            expected_checksum = hashlib.md5(packet_json.encode()).hexdigest()[:8]

            if received_checksum == expected_checksum:
                data['checksum'] = received_checksum  # Put it back
                return True, data
            else:
                self.log_and_print(f"Checksum mismatch!")
                self.log_and_print(f"  Received checksum: {received_checksum}")
                self.log_and_print(f"  Expected checksum: {expected_checksum}")
                self.log_and_print(f"  Packet JSON length: {len(packet_json)}")
                self.log_and_print(f"  First 100 chars: {packet_json[:100]}")
                return False, None
        except Exception as e:
            self.log_and_print(f"Packet verification error: {e}")
            return False, None

    def send_packet(self, packet_data):
        """Send packet via LoRa using the proven method from test script"""
        try:
            packet_size = len(packet_data)
            if packet_size > 240:
                self.log_and_print(f"WARNING: Packet size {packet_size} may exceed LoRa limits")
            
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

    def send_fetch_range_request(self):
        """Compose and send a FETCH_RANGE request."""
        request_id = str(uuid.uuid4())
        self.log_and_print(f"RX: Initiating data fetch request {request_id} for range "
                         f"[{self.fetch_start} to {self.fetch_end}]")
        
        payload = {
            'start_ts': self.fetch_start,
            'end_ts': self.fetch_end,
        }
        
        packet = self.create_packet(
            packet_type='FETCH_RANGE',
            seq_num=self.seq_number,
            payload=payload,
            request_id=request_id
        )
        
        if self.send_packet(packet):
            self.log_and_print(f"RX: FETCH_RANGE request sent for ID {request_id}")
            self.update_stats('fetch_requests_sent')
            self.seq_number += 1
            return True
        else:
            self.log_and_print(f"RX: Failed to send FETCH_RANGE request for ID {request_id}")
            return False

    def handle_fetch_range_request(self, packet_info):
        """
        Handles a FETCH_RANGE request by spawning a new thread to process it.
        This ensures the main receiver loop is not blocked.
        """
        self.log_and_print("TX: Spawning new thread to handle FETCH_RANGE request.")
        self.update_stats('fetch_requests_handled')
        
        thread = threading.Thread(
            target=self._process_fetch_request_thread,
            args=(packet_info,),
            daemon=True
        )
        thread.start()

    def _process_fetch_request_thread(self, packet_info):
        """
        The actual processing logic for a fetch request. Runs in a separate thread.
        Streams the source CSV, filters it, compresses the result, and sends a manifest.
        """
        request_id = packet_info.get('request_id')
        payload = packet_info.get('payload', {})
        start_ts_str = payload.get('start_ts')
        end_ts_str = payload.get('end_ts')
        
        if not all([request_id, start_ts_str, end_ts_str]):
            self.log_and_print(f"TX: Received invalid FETCH_RANGE request. Missing fields.")
            return
        
        self.log_and_print(f"TX: Processing FETCH_RANGE request {request_id} for range [{start_ts_str} to {end_ts_str}]")
        
        try:
            start_dt = datetime.fromisoformat(start_ts_str)
            end_dt = datetime.fromisoformat(end_ts_str)
        except ValueError:
            self.log_and_print(f"TX: Invalid timestamp format in FETCH_RANGE request {request_id}")
            return
        
        first_ts = None
        last_ts = None
        records_found = 0
        gzipped_buffer = io.BytesIO()
        
        try:
            # 1. Open HTTP stream and process line by line
            req = urllib.request.Request(CSV_URL, method="GET")
            req.add_header("Accept", "text/csv, */*;q=0.1")
            req.add_header("User-Agent", "storm3_tx/1.1")
            
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    self.log_and_print(f"TX: Failed to fetch CSV, status {resp.status}")
                    return
                
                # Handle gzipped response
                data = resp.read()
                if resp.headers.get("Content-Encoding", "") == "gzip":
                    data = gzip.decompress(data)
                
                text_data = data.decode('utf-8', errors='replace')
                text_stream = io.StringIO(text_data)
                reader = csv.reader(text_stream)
                
                header = next(reader, None)  # Skip header
                
                # 2. Filter, compress and store results on the fly
                with gzip.GzipFile(fileobj=gzipped_buffer, mode='wb') as gz_writer:
                    with io.TextIOWrapper(gz_writer, encoding='utf-8') as text_writer:
                        # Write header first
                        if header:
                            text_writer.write(",".join(header) + "\n")
                        
                        for row in reader:
                            if len(row) >= 2 and row[0] and row[1]:
                                try:
                                    # Parse date/time - adjust format based on actual CSV format
                                    date_str = row[0].strip()
                                    time_str = row[1].strip()
                                    
                                    # Try different date formats
                                    date_formats = [
                                        "%m/%d/%y %H:%M:%S",
                                        "%m/%d/%Y %H:%M:%S", 
                                        "%Y-%m-%d %H:%M:%S",
                                        "%d/%m/%y %H:%M:%S",
                                        "%d/%m/%Y %H:%M:%S"
                                    ]
                                    
                                    record_dt = None
                                    datetime_str = f"{date_str} {time_str}"
                                    
                                    for fmt in date_formats:
                                        try:
                                            record_dt = datetime.strptime(datetime_str, fmt)
                                            break
                                        except ValueError:
                                            continue
                                    
                                    if record_dt and start_dt <= record_dt < end_dt:
                                        text_writer.write(",".join(row) + "\n")
                                        records_found += 1
                                        record_ts_str = record_dt.isoformat()
                                        if first_ts is None:
                                            first_ts = record_ts_str
                                        last_ts = record_ts_str
                                        
                                except (ValueError, IndexError):
                                    continue  # Skip malformed rows silently in stream
            
            data_bytes = gzipped_buffer.getvalue()
            
        except Exception as e:
            self.log_and_print(f"TX: Error processing CSV stream for {request_id}: {e}")
            return
        
        # 3. Generate and send the manifest
        total_chunks = math.ceil(len(data_bytes) / self.CHUNK_SIZE) if data_bytes else 0
        
        manifest = {
            'records_found': records_found,
            'bytes_estimate': len(data_bytes),
            'chunk_size': self.CHUNK_SIZE,
            'total_chunks': total_chunks,
            'first_ts': first_ts,
            'last_ts': last_ts,
            'content_encoding': 'gzip' if data_bytes else 'none'
        }
        
        with self.transfer_lock:
            self.pending_transfers[request_id] = {
                'data': data_bytes,
                'manifest': manifest,
                'next_chunk_index': 0,
                'total_chunks': total_chunks,
            }
        
        self.log_and_print(f"TX: Prepared {manifest['records_found']} records ({manifest['bytes_estimate']} bytes) for transfer.")
        
        manifest_packet = self.create_packet('MANIFEST', self.seq_number, manifest, request_id)
        if self.send_packet(manifest_packet):
            self.log_and_print(f"TX: Sent MANIFEST for request {request_id} with Seq#{self.seq_number}")
            with self.transfer_lock:
                if request_id in self.pending_transfers:
                    self.pending_transfers[request_id]['manifest_seq'] = self.seq_number
            self.update_stats('packets_sent')
            self.seq_number += 1
        else:
            self.log_and_print(f"TX: Failed to send MANIFEST for request {request_id}")
            with self.transfer_lock:
                self.pending_transfers.pop(request_id, None)

    def handle_manifest(self, packet_info):
        """Handle an incoming MANIFEST packet on the receiver."""
        if self.mode != 'rx':
            return
            
        request_id = packet_info.get('request_id')
        manifest = packet_info.get('payload', {})
        
        if not request_id or not manifest:
            self.log_and_print("RX: Received invalid MANIFEST packet.")
            return
            
        self.log_and_print(f"RX: Received MANIFEST for request {request_id}:")
        self.log_and_print(f"    Records found: {manifest.get('records_found')}")
        self.log_and_print(f"    Total size: {manifest.get('bytes_estimate')} bytes")
        self.log_and_print(f"    Total chunks: {manifest.get('total_chunks')}")
        
        if manifest.get('total_chunks', 0) == 0:
            self.log_and_print(f"RX: Transfer for {request_id} complete (0 chunks).")
            return
            
        with self.transfer_lock:
            self.pending_downloads[request_id] = {
                'manifest': manifest,
                'chunks': {},
                'chunks_received': 0,
                'data': b''  # To store re-assembled data
            }
        
        self.log_and_print(f"RX: Acknowledging MANIFEST. Ready to receive data chunks for {request_id}.")
        
        ack_packet = self.create_packet(
            packet_type='ACK',
            seq_num=packet_info['seq'],
            request_id=request_id
        )
        
        if self.send_packet(ack_packet):
            self.log_and_print(f"RX: Sent ACK for MANIFEST (Seq#{packet_info['seq']})")
            self.update_stats('acks_sent')
        else:
            self.log_and_print(f"RX: Failed to send ACK for MANIFEST (Seq#{packet_info['seq']})")

    def handle_data_chunk(self, packet_info):
        """Handle an incoming DATA_CHUNK packet on the receiver."""
        request_id = packet_info.get('request_id')
        payload = packet_info.get('payload', {})
        chunk_index = payload.get('chunk_index')
        chunk_data_b64 = payload.get('data')
        
        if not request_id:
            self.log_and_print(f"RX: Received chunk without request_id. Discarding.")
            return
            
        with self.transfer_lock:
            if request_id not in self.pending_downloads:
                self.log_and_print(f"RX: Received chunk for unknown/completed request {request_id}. Discarding.")
                return
            download_info = self.pending_downloads[request_id]
        
        if chunk_index is None or chunk_data_b64 is None:
            self.log_and_print(f"RX: Received invalid DATA_CHUNK packet for {request_id}. Discarding.")
            return
        
        if chunk_index not in download_info['chunks']:
            try:
                chunk_data = base64.b64decode(chunk_data_b64)
                with self.transfer_lock:
                    download_info['chunks'][chunk_index] = chunk_data
                    download_info['chunks_received'] += 1
                
                total_chunks = download_info['manifest']['total_chunks']
                self.log_and_print(f"RX: Stored chunk {chunk_index + 1}/{total_chunks} for {request_id} ({len(chunk_data)} bytes)")
                self.update_stats('data_chunks_received')
                
            except (TypeError, ValueError):
                self.log_and_print(f"RX: Failed to decode base64 data for chunk {chunk_index + 1}. Discarding.")
                return  # Don't ACK a corrupted chunk
        else:
            self.log_and_print(f"RX: Received duplicate chunk {chunk_index + 1}. Discarding.")
        
        # Always ACK receipt so the transmitter can move on
        ack_packet = self.create_packet('ACK', packet_info['seq'], request_id=request_id)
        if self.send_packet(ack_packet):
            self.update_stats('acks_sent')
        else:
            self.log_and_print(f"RX: Failed to send ACK for chunk {chunk_index + 1}")
        
        # Check if transfer is complete
        with self.transfer_lock:
            total_chunks = download_info['manifest']['total_chunks']
            if total_chunks > 0 and download_info['chunks_received'] == total_chunks:
                self.log_and_print(f"RX: All {total_chunks} chunks received for {request_id}.")
                self._reassemble_and_save_data(request_id)

    def handle_transfer_done(self, packet_info):
        """Handles a TRANSFER_DONE packet, confirming the end of a transfer."""
        request_id = packet_info.get('request_id')
        self.log_and_print(f"RX: Received TRANSFER_DONE for {request_id}.")
        
        # Acknowledge the final packet
        ack_packet = self.create_packet('ACK', packet_info['seq'], request_id=request_id)
        if self.send_packet(ack_packet):
            self.update_stats('acks_sent')
        else:
            self.log_and_print(f"RX: Failed to send final ACK for {request_id}")
        
        # Verify chunk count and trigger reassembly if it hasn't happened yet
        with self.transfer_lock:
            download_info = self.pending_downloads.get(request_id)
            if download_info:
                total_chunks_manifest = download_info['manifest']['total_chunks']
                if download_info['chunks_received'] != total_chunks_manifest:
                    self.log_and_print(f"RX: WARNING - Transfer for {request_id} ended, but chunk count mismatch.")
                    self.log_and_print(f"RX: Expected {total_chunks_manifest}, received {download_info['chunks_received']}")
                else:
                    self.log_and_print(f"RX: Transfer for {request_id} confirmed complete.")
                
                # Reassembly is normally triggered by the last chunk, but this is a fallback.
                if request_id in self.pending_downloads:
                    self._reassemble_and_save_data(request_id)

    def _reassemble_and_save_data(self, request_id):
        """Reassembles chunks for a completed download and saves to a file."""
        with self.transfer_lock:
            download_info = self.pending_downloads.get(request_id)
            if not download_info:
                return
        
        self.log_and_print(f"RX: Reassembling data for request {request_id}.")
        
        # Sort chunks by index and join them
        sorted_chunks = sorted(download_info['chunks'].items())
        full_data_bytes = b''.join([chunk_data for chunk_index, chunk_data in sorted_chunks])
        
        # Decompress data if necessary, based on the manifest
        content_encoding = download_info['manifest'].get('content_encoding')
        if content_encoding == 'gzip':
            try:
                self.log_and_print(f"RX: Decompressing {len(full_data_bytes)} bytes of gzip data...")
                full_data_bytes = gzip.decompress(full_data_bytes)
            except gzip.BadGzipFile as e:
                self.log_and_print(f"RX: Error decompressing data for {request_id}: {e}")
                with self.transfer_lock:
                    self.pending_downloads.pop(request_id, None)  # Clean up failed download
                return
        
        # Save the reassembled data to a file
        output_filename = os.path.join(LOG_DIR, f"recovered_data_{request_id}.csv")
        try:
            with open(output_filename, "wb") as f:
                f.write(full_data_bytes)
            self.log_and_print(f"RX: Successfully saved reassembled data to {output_filename}")
            self.log_and_print(f"RX: File size: {len(full_data_bytes)} bytes, Records: {download_info['manifest']['records_found']}")
        except IOError as e:
            self.log_and_print(f"RX: Error saving reassembled data for {request_id}: {e}")
        
        # Clean up the completed download from memory
        with self.transfer_lock:
            self.pending_downloads.pop(request_id, None)

    def send_next_chunk(self, request_id):
        """Sends the next available data chunk for a given transfer request."""
        with self.transfer_lock:
            transfer_info = self.pending_transfers.get(request_id)
            if not transfer_info:
                self.log_and_print(f"TX: Cannot send chunk, no pending transfer for {request_id}")
                return
            
            chunk_index = transfer_info['next_chunk_index']
            total_chunks = transfer_info['total_chunks']
        
        if chunk_index >= total_chunks:
            self.log_and_print(f"TX: All chunks sent for {request_id}. Sending TRANSFER_DONE.")
            done_packet = self.create_packet(
                packet_type='TRANSFER_DONE',
                seq_num=self.seq_number,
                request_id=request_id,
                payload={'total_chunks': total_chunks}
            )
            if self.send_packet(done_packet):
                with self.transfer_lock:
                    if request_id in self.pending_transfers:
                        self.pending_transfers[request_id]['done_seq'] = self.seq_number
                self.update_stats('packets_sent')
                self.seq_number += 1
            else:
                self.log_and_print(f"TX: Failed to send TRANSFER_DONE for {request_id}. Cleaning up.")
                with self.transfer_lock:
                    self.pending_transfers.pop(request_id, None)
            return
        
        with self.transfer_lock:
            data = transfer_info['data']
            chunk_size = transfer_info['manifest']['chunk_size']
        
        start = chunk_index * chunk_size
        end = start + chunk_size
        chunk_data = data[start:end]
        
        payload = {
            'chunk_index': chunk_index,
            'total_chunks': total_chunks,
            # base64 encode the binary data to ensure it's valid JSON
            'data': base64.b64encode(chunk_data).decode('ascii')
        }
        
        packet = self.create_packet('DATA_CHUNK', self.seq_number, payload, request_id)
        if self.send_packet(packet):
            self.log_and_print(f"TX: Sent chunk {chunk_index + 1}/{total_chunks} for {request_id} (Seq#{self.seq_number})")
            with self.transfer_lock:
                if request_id in self.pending_transfers:
                    self.pending_transfers[request_id]['last_chunk_seq'] = self.seq_number
                    self.pending_transfers[request_id]['next_chunk_index'] += 1
            self.update_stats('packets_sent')
            self.update_stats('data_chunks_sent')
            self.seq_number += 1
        else:
            self.log_and_print(f"TX: Failed to send chunk {chunk_index + 1}/{total_chunks} for {request_id}")

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

                storm_data = {
                    'board_temp_c': self.get_cpu_temp(),
                    'csv_date': csv_date,
                    'csv_time': csv_time,
                    'riverstage': riverstage,
                    'rain_gauge': rain_gauge,
                    'site_id': SITE_ID
                }

                packet = self.create_packet('STORM_DATA', self.seq_number, storm_data)

                self.log_and_print(f"TX Seq#{self.seq_number}: Sending Storm3 data packet.")

                success = False
                retry_count = 0
                max_retries = self.MAX_RETRIES

                while retry_count < max_retries and not success:
                    if retry_count > 0:
                        self.log_and_print(f"TX Seq#{self.seq_number}: Retry {retry_count}/{max_retries-1}")

                    if self.send_packet(packet):
                        self.log_and_print(f"TX Seq#{self.seq_number}: Packet sent, waiting for ACK...")
                        self.update_stats('packets_sent')

                        self.ack_received.clear()
                        self.pending_acks[self.seq_number] = retry_count

                        if self.ack_received.wait(timeout=self.ACK_TIMEOUT):
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
                        packet_type = packet_info['type']
                        
                        if packet_type == 'STORM_DATA':
                            payload = packet_info.get('payload', {})
                            self.log_and_print(f"RX: STORM_DATA packet Seq#{packet_info['seq']} from {payload.get('site_id', 'Unknown')}")
                            self.log_and_print(f"RX: Date: {payload.get('csv_date')} {payload.get('csv_time')}")
                            self.log_and_print(f"RX: River: {payload.get('riverstage')}, Rain: {payload.get('rain_gauge')}")
                            self.log_and_print(f"RX: Board Temp: {payload.get('board_temp_c')}°C")

                            self.update_stats('packets_received')

                            # Log to CSV
                            try:
                                with open(self.csv_path, "a") as f:
                                    ts = datetime.now().isoformat()
                                    f.write(f"{ts},{packet_info['seq']},{packet_rssi},{noise_rssi},{snr},"
                                            f"{payload.get('board_temp_c', '')}"
                                            f",\"{payload.get('csv_date', '')}\",\"{payload.get('csv_time', '')}\""
                                            f",\"{payload.get('riverstage', '')}\",\"{payload.get('rain_gauge', '')}\""
                                            f",\"{payload.get('site_id', '')}\",\"{packet_info.get('timestamp', '')}\"\n")
                            except Exception as e:
                                self.log_and_print(f"Failed to write to CSV log: {e}")

                            # Send ACK using proven method
                            ack_packet = self.create_packet('ACK', packet_info['seq'])
                            if self.send_packet(ack_packet):
                                self.log_and_print(f"RX: ACK sent for Seq#{packet_info['seq']}")
                                self.update_stats('acks_sent')
                            else:
                                self.log_and_print(f"RX: Failed to send ACK for Seq#{packet_info['seq']}")

                        elif packet_type == 'ACK':
                            seq_num = packet_info['seq']
                            request_id = packet_info.get('request_id')
                            
                            # Check if this ACK is part of a file transfer
                            if self.mode == 'tx' and request_id:
                                with self.transfer_lock:
                                    transfer_info = self.pending_transfers.get(request_id)
                                    if transfer_info:
                                        manifest_seq = transfer_info.get('manifest_seq')
                                        last_chunk_seq = transfer_info.get('last_chunk_seq')
                                        done_seq = transfer_info.get('done_seq')
                                        
                                        if manifest_seq == seq_num:
                                            self.log_and_print(f"TX: Received ACK for MANIFEST for {request_id}. Starting transfer.")
                                            # Remove manifest_seq so we don't process this ACK again
                                            del transfer_info['manifest_seq']
                                            # Start sending chunks
                                            self.send_next_chunk(request_id)
                                        elif last_chunk_seq == seq_num:
                                            chunk_num = transfer_info['next_chunk_index']
                                            total_chunks = transfer_info['total_chunks']
                                            self.log_and_print(f"TX: Received ACK for chunk {chunk_num}/{total_chunks}. Sending next.")
                                            self.send_next_chunk(request_id)
                                        elif done_seq == seq_num:
                                            self.log_and_print(f"TX: Received ACK for TRANSFER_DONE for {request_id}. Transfer successful.")
                                            self.pending_transfers.pop(request_id, None)
                                        else:
                                            self.log_and_print(f"TX: Received stale/unexpected ACK for transfer {request_id} (Seq#{seq_num})")
                                        continue  # Packet handled, go to next iteration
                            
                            # Handle ACKs for regular data packets
                            if seq_num in self.pending_acks:
                                self.log_and_print(f"RX: ACK received for Seq#{seq_num}")
                                self.pending_acks.pop(seq_num)
                                self.ack_received.set()
                            else:
                                self.log_and_print(f"RX: Unexpected ACK for Seq#{seq_num}")

                        elif packet_type == 'FETCH_RANGE':
                            if self.mode == 'tx':
                                self.handle_fetch_range_request(packet_info)

                        elif packet_type == 'MANIFEST':
                            if self.mode == 'rx':
                                self.handle_manifest(packet_info)

                        elif packet_type == 'DATA_CHUNK':
                            if self.mode == 'rx':
                                self.handle_data_chunk(packet_info)

                        elif packet_type == 'TRANSFER_DONE':
                            if self.mode == 'rx':
                                self.handle_transfer_done(packet_info)

                        else:
                            self.log_and_print(f"RX: Unknown packet type: {packet_type}")

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
                # Start receiver thread for ACKs and fetch requests
                rx_thread = threading.Thread(target=self.receiver_loop, daemon=True)
                rx_thread.start()
                # Run transmitter in main thread
                self.transmitter_loop()
            else:  # rx mode
                # If fetch arguments are provided, send the data fetch request
                if self.fetch_start and self.fetch_end:
                    # Give the node a moment to initialize before sending
                    time.sleep(1)
                    self.send_fetch_range_request()
                # Run receiver loop to listen for responses and regular packets
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
    parser.add_argument('--fetch-start', type=str, help='Start timestamp for data fetch (rx mode only, ISO-8601 format)')
    parser.add_argument('--fetch-end', type=str, help='End timestamp for data fetch (rx mode only, ISO-8601 format)')

    args = parser.parse_args()

    # Validate fetch arguments
    if (args.fetch_start and not args.fetch_end) or (not args.fetch_start and args.fetch_end):
        parser.error("--fetch-start and --fetch-end must be used together.")
    
    if args.fetch_start and args.mode == 'tx':
        parser.error("--fetch-start and --fetch-end are only valid in rx mode.")

    # Validate timestamp format if provided
    if args.fetch_start:
        try:
            datetime.fromisoformat(args.fetch_start)
            datetime.fromisoformat(args.fetch_end)
        except ValueError:
            parser.error("Timestamps must be in ISO-8601 format (e.g., '2025-09-15T14:00:00')")

    # Create and run application
    app = LoRaNode(
        mode=args.mode,
        addr=args.addr,
        target_addr=args.target,
        fetch_start=args.fetch_start,
        fetch_end=args.fetch_end
    )

    try:
        app.run()
    except KeyboardInterrupt:
        print("\nApp interrupted by user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
