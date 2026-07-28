"""L2 packet sniffer: passively captures TDX ext-market traffic via WinDivert.

Captures TCP packets on port 7727 from the authenticated TDX client connection,
parses the TDX L2 binary protocol (zlib-compressed), and extracts:
  - Tick-by-tick transactions (逐笔成交)
  - 10-level order book (十档盘口)
  - Minute bar data (分钟K线)

Usage: python -m l2.scripts.l2_sniffer [--duration 30] [--output l2_capture.parquet]
"""

import sys
import struct
import zlib
import time
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("l2_sniffer")

RSP_HEADER_LEN = 0x10

# TDX L2 ext command IDs (from tdxpy source)
CMD_GET_INSTRUMENT_QUOTE = 0x0130
CMD_GET_TRANSACTION_DATA = 0x0111
CMD_GET_MINUTE_TIME_DATA = 0x0106
CMD_GET_INSTRUMENT_COUNT = 0x0100
CMD_GET_INSTRUMENT_INFO = 0x0102
CMD_GET_HISTORY_TRANSACTION = 0x0131
CMD_GET_HISTORY_MINUTE = 0x0107

CMD_NAMES = {
    0x0130: "QUOTE",
    0x0111: "TRANSACTION",
    0x0106: "MINUTE",
    0x0100: "INSTRUMENT_COUNT",
    0x0102: "INSTRUMENT_INFO",
    0x0131: "HISTORY_TRANSACTION",
    0x0107: "HISTORY_MINUTE",
}


def parse_header(head_buf: bytes) -> tuple[int, int, int, int]:
    """Parse TDX response header (0x10 bytes)."""
    if len(head_buf) < RSP_HEADER_LEN:
        return (0, 0, 0, 0)
    _, _, cmd_id, zip_size, unzip_size = struct.unpack("<IIIHH", head_buf)
    return cmd_id, zip_size, unzip_size, _


def parse_transaction(body_buf: bytes, date: int) -> list[dict]:
    """Parse transaction data from TDX L2 format."""
    pos = 0
    if len(body_buf) < 16:
        return []

    market, code, _, num = struct.unpack("<B9s4sH", body_buf[pos: pos + 16])
    code_str = code.decode("gbk", errors="ignore").strip("\x00")
    pos += 16
    results = []

    year = date // 10000
    month = date % 10000 // 100
    day = date % 100

    for _ in range(num):
        if pos + 16 > len(body_buf):
            break
        raw_time, price, volume, zengcang, direction = struct.unpack("<HIIiH", body_buf[pos: pos + 16])
        pos += 16

        hour = raw_time // 60
        minute = raw_time % 60
        second = direction % 10000
        value = direction // 10000

        if second > 59:
            second = 0

        dt = datetime(year, month, day, hour, minute, second)

        if value == 0:
            direction = 1
        elif value == 1:
            direction = -1
        else:
            direction = 0

        # market 31/48 = options/futures
        if market in [31, 48]:
            if direction == 0:
                direction_value = 1 if second == 0 else (-1 if second == 256 else 0)
                direction = direction_value

        results.append({
            "time": dt,
            "price": float(price) / 1000.0,
            "volume": volume,
            "amount": volume * price / 1000.0,
            "direction": direction,
        })

    return results


def parse_quote(body_buf: bytes) -> dict | None:
    """Parse instrument quote from TDX L2 format (simplified)."""
    if len(body_buf) < 20:
        return None
    market, code = struct.unpack("<B9s", body_buf[:10])
    code_str = code.decode("gbk", errors="ignore").strip("\x00")
    return {
        "code": code_str,
        "market": market,
        "raw_len": len(body_buf),
    }


class L2Sniffer:
    """Passive L2 traffic sniffer using PyDivert."""

    L2_SERVER_IPS = [
        "112.74.214.43",
        "120.25.218.6",
        "43.139.173.246",
        "159.75.90.107",
    ]

    def __init__(self, output_dir: str = "data/l2/sniffed"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.stats = defaultdict(int)
        self.packets_captured = 0
        self.payload_bytes = 0
        self.parsed_messages = 0

        # TCP stream reassembly buffer: (src_ip, src_port, dst_ip, dst_port) -> bytearray
        self.stream_buf = defaultdict(bytearray)
        self.stream_seq = defaultdict(int)  # expected next seq

        self.transactions: list[dict] = []
        self.quotes: list[dict] = []

    def build_filter(self) -> str:
        """Build WinDivert filter string for L2 servers."""
        ip_conditions = " or ".join(
            f"ip.DstAddr == {ip} or ip.SrcAddr == {ip}" for ip in self.L2_SERVER_IPS
        )
        return f"tcp.DstPort == 7727 and ({ip_conditions})"

    def capture(self, duration_sec: int = 30):
        """Capture L2 traffic for specified duration."""
        import pydivert

        filter_str = self.build_filter()
        logger.info(f"Filter: {filter_str}")
        logger.info(f"Capturing for {duration_sec}s... (run as Administrator required)")

        start_time = time.time()
        show_interval = 5
        last_show = start_time

        try:
            with pydivert.WinDivert(filter_str) as w:
                for packet in w:
                    elapsed = time.time() - start_time

                    if packet.payload and len(packet.payload) > 0:
                        self._process_packet(packet)

                    # Must re-inject so TDX keeps working
                    w.send(packet)

                    if time.time() - last_show >= show_interval:
                        self._show_stats(elapsed)
                        last_show = time.time()

                    if elapsed >= duration_sec:
                        break

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except PermissionError:
            logger.error("Permission denied - run as Administrator!")
            raise

        self._show_stats(time.time() - start_time, final=True)
        self._save_results()

    def _process_packet(self, packet):
        """Process captured packet payload - try to parse TDX protocol."""
        self.packets_captured += 1
        self.payload_bytes += len(packet.payload)

        # Try to parse as TDX response header
        payload = bytes(packet.payload)

        if len(payload) >= RSP_HEADER_LEN:
            # Check for possible TDX response header
            # First 4 bytes: zeros (reserved), next 4: zeros (reserved),
            # next 4: cmd_id, then zip_size, then unzip_size
            try:
                reserved1, _, cmd_id, zip_size, unzip_size = struct.unpack(
                    "<IIIHH", payload[:RSP_HEADER_LEN]
                )

                # Valid TDX cmd: zip_size > 0 and reasonable
                if 0 < zip_size < 500000 and cmd_id > 0:
                    self.stats[f"cmd_{cmd_id:04X}"] += 1

                    body = payload[RSP_HEADER_LEN:]
                    if len(body) >= zip_size:
                        body = body[:zip_size]

                    if zip_size != unzip_size and len(body) >= zip_size:
                        try:
                            body = zlib.decompress(body)
                        except zlib.error:
                            pass  # Not compressed or malformed

                    self._parse_body(cmd_id, body)
                    self.parsed_messages += 1

            except struct.error:
                pass  # Not enough data or malformed

    def _parse_body(self, cmd_id: int, body: bytes):
        """Parse decompressed body based on command type."""
        cmd_name = CMD_NAMES.get(cmd_id, f"UNKNOWN_{cmd_id:04X}")

        if cmd_id == CMD_GET_TRANSACTION_DATA or cmd_id == CMD_GET_HISTORY_TRANSACTION:
            # Determine date - for realtime, use today
            today = int(datetime.now().strftime("%Y%m%d"))
            ticks = parse_transaction(body, today)
            if ticks:
                self.transactions.extend(ticks)
                logger.info(f"  [{cmd_name}] {len(ticks)} ticks parsed")

        elif cmd_id == CMD_GET_INSTRUMENT_QUOTE:
            q = parse_quote(body)
            if q:
                self.quotes.append(q)
                logger.info(f"  [{cmd_name}] Quote for {q['code']} ({q['raw_len']} bytes)")

        elif cmd_id == CMD_GET_INSTRUMENT_COUNT:
            if len(body) >= 4:
                count = struct.unpack("<I", body[:4])[0]
                logger.info(f"  [INSTRUMENT_COUNT] {count:,}")

        elif cmd_id == CMD_GET_MINUTE_TIME_DATA or cmd_id == CMD_GET_HISTORY_MINUTE:
            logger.info(f"  [{cmd_name}] Minute/bar data ({len(body)} bytes)")

    def _show_stats(self, elapsed: float, final: bool = False):
        """Show capture statistics."""
        prefix = "FINAL" if final else "STATS"
        rate = self.payload_bytes / elapsed / 1024 if elapsed > 0 else 0
        logger.info(
            f"[{prefix}] {elapsed:.0f}s | "
            f"packets: {self.packets_captured:,} | "
            f"payload: {self.payload_bytes / 1024:.0f} KB | "
            f"rate: {rate:.1f} KB/s | "
            f"parsed: {self.parsed_messages:,} msgs | "
            f"ticks: {len(self.transactions):,} | "
            f"quotes: {len(self.quotes):,}"
        )

        if final and self.stats:
            logger.info("Cmd stats:")
            for cmd, count in sorted(self.stats.items()):
                logger.info(f"  {cmd}: {count}")

    def _save_results(self):
        """Save captured data to Parquet."""
        import pandas as pd

        if self.transactions:
            df = pd.DataFrame(self.transactions)
            path = self.output_dir / f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.to_parquet(path)
            logger.info(f"Saved {len(df)} transactions to {path}")

        if self.quotes:
            df = pd.DataFrame(self.quotes)
            path = self.output_dir / f"quotes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            df.to_parquet(path)
            logger.info(f"Saved {len(df)} quotes to {path}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="TDX L2 Packet Sniffer")
    parser.add_argument("--duration", type=int, default=30, help="Capture duration (seconds)")
    parser.add_argument("--output", type=str, default="data/l2/sniffed", help="Output directory")
    args = parser.parse_args()

    sniffer = L2Sniffer(output_dir=args.output)
    sniffer.capture(duration_sec=args.duration)


if __name__ == "__main__":
    main()
