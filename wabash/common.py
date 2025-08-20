# wabash/common.py
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import time

try:
    import sx126x
except ImportError:
    sx126x = None

# --- LoRa / Radio Configuration ---
SERIAL_PORT = "/dev/serial0"
FREQ_MHZ = 915
AIR_SPEED = 1200
POWER_DBM = 22

# --- Address Configuration ---
# Transmitter is 1, Receiver is 2
ADDR_TX = 1
ADDR_RX = 2

# --- Packet Structure Helpers ---
def freq_offset(mhz: int) -> int:
    """Convert frequency to the offset byte used by the SX126x header."""
    return int(mhz - (850 if mhz > 850 else 410))

def build_header(dst_addr: int, src_addr: int) -> bytes:
    """
    Build the 6-byte header: dst(2) dst_off(1) src(2) src_off(1).
    Frequency offset is derived automatically from FREQ_MHZ.
    """
    dst_off = freq_offset(FREQ_MHZ)
    src_off = dst_off
    return bytes([
        (dst_addr >> 8) & 0xFF, dst_addr & 0xFF, dst_off & 0xFF,
        (src_addr >> 8) & 0xFF, src_addr & 0xFF, src_off & 0xFF,
    ])

# --- System & Initialization Helpers ---
def ensure_driver() -> None:
    """Abort if the sx126x driver is missing."""
    if sx126x is None:
        # Use a basic logger setup in case this is called before full logging is configured
        logging.basicConfig(level=logging.CRITICAL, format="%(asctime)sZ %(levelname)s %(message)s")
        logging.critical("sx126x module not installed; cannot continue.")
        sys.exit(2)

def setup_logging(log_name: str, base_dir: str, level=logging.INFO, max_bytes=300_000) -> logging.Logger:
    """Sets up a rotating file logger."""
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, f"{log_name}.log")

    logger = logging.getLogger(log_name)
    logger.setLevel(level)

    handler = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=3)
    fmt = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    fmt.converter = time.gmtime
    handler.setFormatter(fmt)

    # Avoid adding handlers multiple times if the logger is already configured
    if not logger.handlers:
        logger.addHandler(handler)

    return logger

def init_radio(my_addr: int, for_tx: bool):
    """Initializes the sx126x radio node."""
    ensure_driver()
    try:
        node = sx126x.sx126x(
            serial_num=SERIAL_PORT,
            freq=FREQ_MHZ,
            addr=my_addr,
            power=POWER_DBM,
            rssi=not for_tx,  # RSSI enabled for RX, disabled for TX
            air_speed=AIR_SPEED,
            relay=False
        )
        return node
    except Exception as e:
        # The logger might not be fully configured yet if this fails,
        # so we also print to stderr.
        print(f"FATAL: Radio init failed: {e}", file=sys.stderr)
        # Attempt to log, in case logging was set up
        logging.getLogger().critical(f"Radio init failed: {e}", exc_info=True)
        sys.exit(3)
