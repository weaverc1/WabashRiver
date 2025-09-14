# LoRa App Command-Line Interface (CLI) Help

This document provides a comprehensive guide to the command-line arguments for the `lora_app.py` script. The script can be run in two main modes: transmitter (`tx`) and receiver (`rx`), each with its own set of specific options.

## Usage

The basic syntax for running the script is:

```bash
python lora_app.py [mode] [options]
```

### Modes

There are two operational modes available:

-   `tx`: **Transmitter Mode**. In this mode, the script fetches the latest data from a Storm3 device and broadcasts it over LoRa.
-   `rx`: **Receiver Mode**. In this mode, the script listens for incoming LoRa packets, acknowledges them, and logs the received data.

---

## Command-Line Arguments

Below is a detailed list of all available command-line arguments.

### Required Arguments

-   **`mode`**
    -   **Description**: Specifies the operational mode of the application.
    -   **Values**: `tx` or `rx`.
    -   **Example**: `python lora_app.py tx ...`

-   **`--addr`**
    -   **Description**: Sets the LoRa address for the current device. This address must be a unique integer that identifies this node on the network.
    -   **Example**: `python lora_app.py tx --addr 1`

### Optional Arguments

-   **`--target`**
    -   **Description**: Defines the LoRa address of the target device to which packets will be sent (in `tx` mode) or from which packets are expected (in `rx` mode). If not specified, it defaults to `2` for `tx` mode and `1` for `rx` mode.
    -   **Example**: `python lora_app.py tx --addr 1 --target 2`

-   **`--fetch-start`**
    -   **Description**: **(Receiver mode only)** Sets the start timestamp for a historical data fetch request. When used, the receiver will request a data log from the transmitter beginning at this time. It must be provided in ISO-8601 format (`YYYY-MM-DDTHH:MM:SS`). This argument must be used in conjunction with `--fetch-end`.
    -   **Example**: `python lora_app.py rx --addr 2 --fetch-start "2023-10-26T10:00:00"`

-   **`--fetch-end`**
    -   **Description**: **(Receiver mode only)** Sets the end timestamp for a historical data fetch request. It defines the end of the time range for the data log being requested. It must also be in ISO-8601 format.
    -   **Example**: `python lora_app.py rx --addr 2 --fetch-start "2023-10-26T10:00:00" --fetch-end "2023-10-26T12:00:00"`

---

## Usage Examples

### Transmitter Mode

-   **Run as a transmitter with address `1`, sending to the default target address `2`**:
    ```bash
    python lora_app.py tx --addr 1
    ```

-   **Run as a transmitter with address `10`, explicitly sending to a receiver at address `20`**:
    ```bash
    python lora_app.py tx --addr 10 --target 20
    ```

### Receiver Mode

-   **Run as a receiver with address `2`, listening for packets from the default target address `1`**:
    ```bash
    python lora_app.py rx --addr 2
    ```

-   **Run as a receiver with address `20`, listening for packets from a transmitter at address `10`**:
    ```bash
    python lora_app.py rx --addr 20 --target 10
    ```

-   **Run as a receiver and request a historical data log for a specific two-hour window**:
    ```bash
    python lora_app.py rx --addr 2 --fetch-start "2023-10-26T10:00:00" --fetch-end "2023-10-26T12:00:00"
    ```

---
