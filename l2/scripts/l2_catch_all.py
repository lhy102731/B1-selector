"""Catch all TDX traffic - capture packets from all TDX connections.

Usage: python l2/scripts/l2_catch_all.py [duration_sec]
"""
import sys
import struct
import zlib
import time
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("tdx_catchall")

RSP_HEADER_LEN = 0x10


def hexdump(data: bytes, max_len: int = 80) -> str:
    lines = []
    for i in range(0, min(len(data), max_len), 16):
        chunk = data[i: i + 16]
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"  {i:04x}  {hex_part:<48s} |{ascii_part}|")
    if len(data) > max_len:
        lines.append(f"  ... ({len(data) - max_len} more bytes)")
    return "\n".join(lines)


def main():
    import pydivert

    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 30

    # Capture ALL external TCP traffic from TDX (PID 26380), not just port 7727
    # TDX uses multiple servers on various ports
    filter_str = (
        "tcp and ("
        " tcp.DstPort == 7727 or tcp.SrcPort == 7727 or "
        " tcp.DstPort == 7709 or tcp.SrcPort == 7709 or "
        " tcp.DstPort == 7615 or tcp.SrcPort == 7615 or "
        " tcp.DstPort == 7719 or tcp.SrcPort == 7719"
        ")"
    )
    logger.info(f"Filter: {filter_str}")
    logger.info(f"Capturing for {duration}s...")

    stats = defaultdict(int)
    streams = defaultdict(list)
    start = time.time()
    pkt_count = 0

    try:
        with pydivert.WinDivert(filter_str) as w:
            for packet in w:
                pkt_count += 1
                elapsed = time.time() - start

                if packet.payload and len(packet.payload) > 0:
                    p = bytes(packet.payload)
                    direction = "OUT" if packet.dst_port in (7727, 7709, 7615, 7719) else "IN "
                    key = f"{packet.dst_port}:{direction}"
                    stats[key] += 1

                    if len(p) > 10:  # Skip tiny ACK packets
                        streams[f"{packet.src_port}->{packet.dst_port}:{direction}"].append(p)
                        logger.info(f"\n--- [{direction}] {packet.src_port} -> {packet.dst_port}:{packet.dst_port}  {len(p)}B ---")
                        logger.info(hexdump(p))

                w.send(packet)

                if elapsed >= duration:
                    break

    except KeyboardInterrupt:
        pass

    elapsed = time.time() - start
    logger.info(f"\n{'='*60}")
    logger.info(f"Done. {elapsed:.0f}s | {pkt_count} total packets")
    for k, v in sorted(stats.items()):
        logger.info(f"  {k}: {v} packets with payload")


if __name__ == "__main__":
    main()
