"""Long-lived, credential-safe bridge to the Tonghuashun Yuanhang quote stack."""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
from pathlib import Path
from typing import Any


class YuanhangBridgeError(RuntimeError):
    """The isolated Yuanhang process failed to start or serve a request."""


ROOT = Path(__file__).resolve().parents[1]
BRIDGE_DIR = ROOT / "tools" / "ths_yuanhang_bridge"
BRIDGE_DLL = BRIDGE_DIR / "YuanhangBridge.dll"
BRIDGE_SOURCE = BRIDGE_DIR / "YuanhangBridge.cs"
BUILD_SCRIPT = BRIDGE_DIR / "build.ps1"


def _windows_user_env(name: str) -> str | None:
    if os.name != "nt":
        return None
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value, _ = winreg.QueryValueEx(key, name)
        return str(value) if value else None
    except (ImportError, FileNotFoundError, OSError):
        return None


def hydrate_ths_process_environment() -> None:
    """Copy missing THS credentials from the Windows user environment.

    Codex and other already-running desktop processes do not automatically
    inherit environment variables configured after they started.  THSDK reads
    credentials from the current process, so hydrate only missing values and
    never log or return their contents.
    """
    for name in ("THS_USERNAME", "THS_PASSWORD", "THS_MAC"):
        if os.environ.get(name):
            continue
        value = _windows_user_env(name)
        if value:
            os.environ[name] = value


def _credential_env() -> dict[str, str]:
    child = os.environ.copy()
    for name in ("THS_USERNAME", "THS_PASSWORD", "THS_MAC"):
        if not child.get(name):
            value = _windows_user_env(name)
            if value:
                child[name] = value
    missing = [name for name in ("THS_USERNAME", "THS_PASSWORD") if not child.get(name)]
    if missing:
        raise YuanhangBridgeError(f"missing environment variables: {', '.join(missing)}")
    return child


class YuanhangHistoryBridge:
    """Own one Yuanhang login and exchange JSON lines with the isolated process."""

    def __init__(
        self,
        *,
        primary_dir: str | Path | None = None,
        dependency_dir: str | Path | None = None,
        snappy_dir: str | Path | None = None,
        startup_timeout: float = 60.0,
        request_timeout: float = 45.0,
    ) -> None:
        self.primary_dir = Path(
            primary_dir
            or os.getenv("THS_YUANHANG_PRIMARY_DIR")
            or r"D:\BaiduNetdiskDownload\ths\ths\Hevo.Sdk\lib"
        )
        self.dependency_dir = Path(
            dependency_dir
            or os.getenv("THS_YUANHANG_DEP_DIR")
            or r"D:\BaiduNetdiskDownload\ths\ths\Hevo.Api.Quotes\lib"
        )
        self.snappy_dir = Path(
            snappy_dir
            or os.getenv("THS_YUANHANG_SNAPPY_DIR")
            or r"D:\ths\同花顺\HevoSpace"
        )
        self.startup_timeout = float(startup_timeout)
        self.request_timeout = float(request_timeout)
        self._process: subprocess.Popen[str] | None = None
        self._output: queue.Queue[str | None] = queue.Queue()
        self._reader: threading.Thread | None = None
        self._lock = threading.RLock()

    @staticmethod
    def _ensure_built() -> None:
        needs_build = not BRIDGE_DLL.exists()
        if BRIDGE_DLL.exists() and BRIDGE_SOURCE.exists():
            needs_build = BRIDGE_DLL.stat().st_mtime < BRIDGE_SOURCE.stat().st_mtime
        if not needs_build:
            return
        completed = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(BUILD_SCRIPT),
            ],
            cwd=BRIDGE_DIR,
            text=True,
            capture_output=True,
            timeout=60,
            check=False,
        )
        if completed.returncode != 0 or not BRIDGE_DLL.exists():
            detail = (completed.stderr or completed.stdout or "unknown compiler error").strip()
            raise YuanhangBridgeError(f"failed to build Yuanhang bridge: {detail[:800]}")

    def _reader_loop(self, process: subprocess.Popen[str]) -> None:
        assert process.stdout is not None
        try:
            for line in process.stdout:
                self._output.put(line.rstrip("\r\n"))
        finally:
            self._output.put(None)

    def _read_json(self, timeout: float) -> dict[str, Any]:
        while True:
            try:
                line = self._output.get(timeout=timeout)
            except queue.Empty as exc:
                raise YuanhangBridgeError("timed out waiting for Yuanhang bridge") from exc
            if line is None:
                code = self._process.poll() if self._process is not None else None
                raise YuanhangBridgeError(f"Yuanhang bridge exited unexpectedly (code={code})")
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload

    def start(self) -> dict[str, Any]:
        with self._lock:
            if self._process is not None and self._process.poll() is None:
                return {"ok": True, "type": "ready"}
            self._ensure_built()
            for directory in (self.primary_dir, self.dependency_dir, self.snappy_dir):
                if not directory.is_dir():
                    raise YuanhangBridgeError(f"Yuanhang library directory not found: {directory}")
            env = _credential_env()
            env.update(
                {
                    "YUANHANG_PRIMARY_DIR": str(self.primary_dir),
                    "YUANHANG_DEP_DIR": str(self.dependency_dir),
                    "YUANHANG_SNAPPY_DIR": str(self.snappy_dir),
                }
            )
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
            self._output = queue.Queue()
            self._process = subprocess.Popen(
                ["dotnet", str(BRIDGE_DLL)],
                cwd=BRIDGE_DIR,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                bufsize=1,
                creationflags=creationflags,
            )
            self._reader = threading.Thread(
                target=self._reader_loop,
                args=(self._process,),
                name="ths-yuanhang-reader",
                daemon=True,
            )
            self._reader.start()
            ready = self._read_json(self.startup_timeout)
            if not ready.get("ok") or ready.get("type") != "ready":
                self.close()
                raise YuanhangBridgeError(
                    f"Yuanhang login failed: {ready.get('error_type')}: {ready.get('error')}"
                )
            return ready

    def query(self, request: str) -> list[dict[str, Any]]:
        if not isinstance(request, str) or not request.startswith("id="):
            raise ValueError("Yuanhang request must start with id=")
        with self._lock:
            self.start()
            assert self._process is not None and self._process.stdin is not None
            try:
                self._process.stdin.write(
                    json.dumps({"op": "query", "request": request}, ensure_ascii=False) + "\n"
                )
                self._process.stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                raise YuanhangBridgeError("Yuanhang bridge pipe is unavailable") from exc
            response = self._read_json(self.request_timeout)
            if not response.get("ok"):
                raise YuanhangBridgeError(
                    f"Yuanhang query failed: {response.get('error_type')}: {response.get('error')}"
                )
            rows = response.get("rows", [])
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise YuanhangBridgeError("Yuanhang bridge returned malformed rows")
            return rows

    def close(self) -> None:
        with self._lock:
            process = self._process
            self._process = None
            if process is None:
                return
            if process.poll() is None and process.stdin is not None:
                try:
                    process.stdin.write('{"op":"shutdown"}\n')
                    process.stdin.flush()
                    process.wait(timeout=5)
                except (BrokenPipeError, OSError, subprocess.TimeoutExpired):
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()

    def __enter__(self) -> "YuanhangHistoryBridge":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()


__all__ = [
    "YuanhangHistoryBridge",
    "YuanhangBridgeError",
    "hydrate_ths_process_environment",
]
