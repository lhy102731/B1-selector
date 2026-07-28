"""Quick L2 traffic dump - capture and display raw protocol structure.

Usage: python l2_sniffer_v2.py [duration_sec]
"""
import sys
import struct
import zlib
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("l2_dump")

RSP_HEADER_LEN = 0x10

CMD_KNOWN = {
    0x0100: "GET_INSTRUMENT_COUNT",
    0x0102: "GET_INSTRUMENT_INFO",
    0x0106: "GET_MINUTE_TIME_DATA",
    0x0107: "GET_HISTORY_MINUTE_DATA",
    0x0111: "GET_TRANSACTION_DATA",
    0x0130: "GET_INSTRUMENT_QUOTE",
    0x0131: "GET_HISTORY_TRANSACTION_DATA",
}


def hexdump(data: bytes, offset: int = 0, max_len: int = 128) -> str:
    """Format bytes as hexdump."""
    data = data[:max_len]
    lines = []
    for i in range(0, len(data), 16):
        chunk = data[i: i + 16]
        hex_part = " ".join(f"{b:02x}" for b in chunk)
        ascii_part = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"  {offset + i:08x}  {hex_part:<48s}  |{ascii_part}|")
    if len(data) < max_len:
        return "\n".join(lines)
    else:
        return "\n".join(lines) + f"\n  ... ({max_len} of {len(data)} bytes)"


def parse_header(head_buf: bytes) -> dict:
    """Parse TDX response header."""
    if len(head_buf) < RSP_HEADER_LEN:
        return {"error": f"too short: {len(head_buf)} bytes"}
    r1, r2, cmd_id, zip_size, unzip_size = struct.unpack("<IIIHH", head_buf)
    return {
        "reserved1": r1,
        "reserved2": r2,
        "cmd_id": f"0x{cmd_id:04X}",
        "cmd_name": CMD_KNOWN.get(cmd_id, f"UNKNOWN"),
        "zip_size": zip_size,
        "unzip_size": unzip_size,
        "compressed": zip_size != unzip_size,
    }


def main():
    import pydivert

    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    logger.info(f"Capturing ALL port 7727 traffic for {duration}s")
    logger.info("Open a stock detail/K-line view in TDX to generate L2 traffic!")

    filter_str = "tcp.DstPort == 7727 or tcp.SrcPort == 7727"
    logger.info(f"Filter: {filter_str}")

    packet_count = 0
    payload_count = 0
    start = time = __import__("time").time
    t0 = start()

    try:
        with pydivert.WinDivert(filter_str) as w:
            for packet in w:
                packet_count += 1
                elapsed = start() - t0

                if packet.payload and len(packet.payload) > 0:
                    payload_count += 1
                    p = bytes(packet.payload)
                    direction = "OUT" if packet.dst_port == 7727 else "IN "
                    src = f"{packet.src_addr}:{packet.src_port}"
                    dst = f"{packet.dst_addr}:{packet.dst_port}"

                    logger.info(f"\n{'='*60}")
                    logger.info(
                        f"[{direction}] pkt#{payload_count}  {src} -> {dst}  "
                        f"payload={len(p)}B"
                    )

                    # Try TDX response header
                    if len(p) >= 16 and direction == "IN ":
                        hdr = parse_header(p[:16])
                        cmd = hdr.get("cmd_name", "?")
                        zs = hdr.get("zip_size", 0)
                        uzs = hdr.get("unzip_size", 0)
                        logger.info(
                            f"  Header: cmd={cmd} zip={zs} unzip={uzs} "
                            f"compressed={hdr.get('compressed')}"
                        )

                        # Try to decompress and parse body
                        if zs > 0 and zs < 500000:
                            body = p[16: 16 + zs]
                            if hdr.get("compressed") and len(body) >= zs:
                                try:
                                    body = zlib.decompress(body)
                                    logger.info(f"  Decompressed: {len(body)} bytes")
                                except zlib.error as e:
                                    logger.info(f"  Decompress failed: {e}")

                            if body and "TRANSACTION" in cmd:
                                # Parse first few ticks
                                if len(body) >= 16:
                                    try:
                                        mk, code, _, num = struct.unpack("<B9s4sH", body[:16])
                                        code_s = code.decode("gbk", errors="ignore").strip("\x00")
                                        logger.info(f"  Transaction: {code_s} x{num} ticks, market={mk}")
                                        # Show first tick
                                        if num > 0 and len(body) >= 32:
                                            t, pr, vol, zc, dr = struct.unpack("<HIIiH", body[16:32])
                                            h = t // 60
                                            m = t % 60
                                            logger.info(
                                                f"  First tick: {h:02d}:{m:02d} "
                                                f"price={pr/1000:.2f} vol={vol}"
                                            )
                                    except struct.error as e:
                                        logger.info(f"  Parse error: {e}")

                    # Show raw hex
                    logger.info(f"\n{hexdump(p, max_len=96)}")

                w.send(packet)

                if elapsed >= duration:
                    break

    except KeyboardInterrupt:
        pass

    elapsed = start() - t0
    logger.info(
        f"\nDone. {elapsed:.0f}s | {packet_count} packets | "
        f"{payload_count} with payload"
    )


if __name__ == "__main__":
    main()
