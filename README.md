# LoRa Storm3 Data Transmission Application

This application provides a robust solution for transmitting data from a Storm3 device over a LoRa link. It consists of two main components: a transmitter (`tx`) and a receiver (`rx`), which can be run on separate devices (e.g., Raspberry Pis equipped with LoRa modules).

The primary purpose is to reliably fetch the latest sensor readings from a Storm3 device's CSV data export and transmit them to a remote receiver. It also includes an advanced feature for requesting and transferring historical data ranges.

## Features

- **Transmitter & Receiver Modes**: Can be configured to act as either a data source or a data sink.
- **Reliable Transmission**: Implements a custom ACK (acknowledgment) and retry mechanism to ensure data packets are received.
- **Stateful Transmitter**: The transmitter saves its state, preventing re-transmission of old data and efficiently fetching only new data using `If-Modified-Since` HTTP headers.
- **Historical Data Fetch**: The receiver can request a specific date range of data from the transmitter. The data is streamed, compressed, and sent in chunks to handle large datasets efficiently.
- **Detailed Logging**: Both modes generate comprehensive logs, including packet info, RSSI/SNR metrics, and operational status.
- **Statistics Reporting**: Tracks and logs key performance indicators like packet success rate, uptime, and average signal strength.
- **Robust Packet Protocol**: Uses a custom JSON-based packet structure with a checksum to ensure data integrity.
- **Cross-Platform**: Written in Python, compatible with Linux environments like Raspberry Pi OS.

## Requirements

### Hardware
- Two LoRa-capable devices (e.g., Raspberry Pi).
- A LoRa module compatible with the `sx126x` library, connected via serial (`/dev/serial0`).
- Network access for the transmitter to reach the Storm3 device.

### Software
- Python 3
- The `pyserial` library.
- The `sx126x` Python library for LoRa communication.

You can install the required Python libraries using pip:
```bash
pip install pyserial sx126x
```

## Configuration

The main configuration variables are located at the top of `lora_app.py`:

- `SITE_ID`: The identifier for the data site (e.g., "Hutsonville"). This is used to construct the CSV data URL.
- `STORM_HOST`: The IP address or hostname of the Storm3 device.
- `CSV_URL`: The full URL to the CSV data file, constructed from `STORM_HOST` and `SITE_ID`.

- `BASE_DIR`: The base directory (`~/storm`) where logs and state files are stored.


## Directory Structure

The application creates the following directory structure:


- `~/storm/`

  - `logs/`: Contains log files for both transmitter and receiver, as well as any received data.
    - `lora_tx.log`: Log for the transmitter.
    - `lora_rx.log`: Log for the receiver.
    - `rx_data.csv`: CSV file where the receiver logs all successfully received `STORM_DATA` packets.
    - `recovered_data_*.csv`: Files containing historical data fetched from the transmitter.
  - `state/`: Stores the transmitter's operational state.
    - `state.json`: A JSON file containing the sequence number and the `Last-Modified` timestamp of the last fetched CSV file. This ensures the transmitter only sends new data.

## Usage

The script is run from the command line, specifying the mode (`tx` or `rx`) and the device's LoRa address.

### Transmitter Mode

The transmitter fetches data every 30 seconds and broadcasts it.

**Command:**
```bash
python lora_app.py tx --addr <local_address> [--target <remote_address>]
```
- `<local_address>`: The LoRa address for this device (e.g., `1`).
- `<remote_address>`: The address of the receiver. Defaults to `2` if not specified.

**Example:**
```bash
python lora_app.py tx --addr 1
```

### Receiver Mode

The receiver listens for data broadcasts from the transmitter.

**Command:**
```bash
python lora_app.py rx --addr <local_address> [--target <remote_address>]
```
- `<local_address>`: The LoRa address for this device (e.g., `2`).
- `<remote_address>`: The address of the transmitter. Defaults to `1` if not specified.

**Example:**
```bash
python lora_app.py rx --addr 2
```

### Historical Data Fetch (Receiver)

To request a historical data range, run the receiver with the `--fetch-start` and `--fetch-end` arguments. The receiver will send a request to the transmitter, which will then stream the requested data.

**Command:**
```bash
python lora_app.py rx --addr <local_address> --fetch-start "YYYY-MM-DDTHH:MM:SS" --fetch-end "YYYY-MM-DDTHH:MM:SS"
```

**Example:**
```bash
# Request data for a full day
python lora_app.py rx --addr 2 --fetch-start "2023-10-26T00:00:00" --fetch-end "2023-10-27T00:00:00"
```

## Communication Protocol

The application uses a custom JSON-based protocol for all LoRa communication. Each packet includes metadata like source/destination addresses, a sequence number, a timestamp, and a checksum for integrity.

### Packet Types

- `STORM_DATA`: The standard packet sent by the transmitter, containing the latest row from the Storm3 CSV file.
- `ACK`: Sent by the receiver to acknowledge receipt of a packet. Essential for the reliable delivery mechanism.
- `FETCH_RANGE`: A special request sent by the receiver to ask the transmitter for historical data.
- `MANIFEST`: The transmitter's response to a `FETCH_RANGE` request. It contains metadata about the requested data, such as the total number of records, estimated size, and number of chunks.
- `DATA_CHUNK`: A chunk of the historical data, sent by the transmitter after the receiver ACKs the `MANIFEST`.
- `TRANSFER_DONE`: Sent by the transmitter to signal the end of a chunked data transfer.