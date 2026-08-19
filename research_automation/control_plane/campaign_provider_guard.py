"""C0 offline provider child bootstrap (CR-010 A4).

Under the offline C0 Guard contract, a provider child MUST be launched
through this fixed bootstrap: the child installs ``NetworkGuard`` and runs
the deny probe BEFORE any provider code is imported or executed, then
lazy-unpickles the provider and executes ``invoke()``.  The module body
performs no network access and imports nothing sensitive at import time --
``campaign.py``'s multiprocessing target is never used for a guarded
provider child, because ``spawn`` imports the target module before the
target function runs.
"""

from __future__ import annotations

import json
import os

_GUARD_RECEIPT_SCHEMA = "control_plane.c0_provider_guard_receipt.v1"


def _provider_identity_started_at_ns() -> int:
    try:
        import psutil as _psutil

        return int(_psutil.Process().create_time() * 1_000_000_000)
    except Exception:  # noqa: BLE001 -- provider child identity best effort
        return 0


def guarded_provider_worker(
    provider_pickle: bytes,
    request_bytes: bytes,
    response_connection: object,
    guard_receipt_connection: object,
) -> None:
    """Spawn target: install the Guard, send the guard receipt, then
    lazy-unpickle the provider and invoke it (never imports campaign.py)."""
    from research_automation.control_plane.rollout_chaos_worker import (
        NetworkGuard,
    )
    from research_automation.control_plane.campaign_provider_guard import (
        _provider_identity_started_at_ns,
    )

    NetworkGuard.install()
    NetworkGuard.deny_probe()
    deny_attempts_before = NetworkGuard.attempts
    receipt = {
        "schema_version": _GUARD_RECEIPT_SCHEMA,
        "guard_installed": True,
        "pid": os.getpid(),
        "started_at_ns": _provider_identity_started_at_ns(),
        "deny_probe_attempts": deny_attempts_before,
        "real_network_attempts": 0,
    }
    try:
        guard_receipt_connection.send(receipt)
    finally:
        try:
            guard_receipt_connection.close()
        except Exception:  # noqa: BLE001
            pass
    import pickle

    try:
        provider = pickle.loads(provider_pickle)
    except Exception:  # noqa: BLE001
        response_connection.send_bytes(
            json.dumps({"v": 2, "tag": "protocol_error"}).encode("ascii")
        )
        return
    try:
        request = json.loads(request_bytes)
    except Exception:  # noqa: BLE001
        response_connection.send_bytes(
            json.dumps({"v": 2, "tag": "protocol_error"}).encode("ascii")
        )
        return
    try:
        try:
            response = provider.invoke(request)
        except TimeoutError:
            response_connection.send_bytes(
                json.dumps({"v": 2, "tag": "provider_timeout"}).encode("ascii")
            )
            return
        except Exception as error:  # noqa: BLE001
            import sys as _sys
            import traceback as _tb

            _tb.print_exc(file=_sys.stderr)
            response_connection.send_bytes(
                json.dumps({"v": 2, "tag": "provider_exception",
                            "detail": str(error)}).encode("ascii")
            )
            return
        output_text = getattr(response, "output_text", None)
        request_model = getattr(response, "request_model", None)
        response_model = getattr(response, "response_model", None)
        usage = getattr(response, "raw_usage", None)
        frame = json.dumps(
            {
                "v": 2,
                "tag": "response",
                "snapshot": {
                    "output_text": str(output_text) if output_text else None,
                    "request_model": str(request_model),
                    "response_model": str(response_model),
                    "raw_usage": dict(usage) if isinstance(usage, dict) else None,
                    "provider_invoked": True,
                },
            }
        )
        response_connection.send_bytes(frame.encode("ascii"))
    finally:
        try:
            response_connection.close()
        except Exception:  # noqa: BLE001
            pass
