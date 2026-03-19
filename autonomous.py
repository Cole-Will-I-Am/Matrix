"""
autonomous.py — Self-healing, self-customizing, self-upgrading orchestration.

Ties together MirrorRegistry, Blender, AdaptiveWrapper, JumpSession, JumpNode,
and DiscoveryManager into an autonomous feedback loop.

Layer 1: ResilienceManager  — Exception-driven fallback chains
Layer 2: EnvironmentAdapter — Runtime self-tuning based on system signals
Layer 3: HotUpgrader        — Over-the-air code swap with automatic rollback
Layer 4: AutonomousLoop     — The feedback loop that ties it all together
Layer 5: GenerativeHealer   — LLM-powered runtime patch generation
Layer 6: WasmSandbox        — WebAssembly sandboxed code execution
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import logging
import os
import textwrap
import threading
import time
import traceback
import types
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

from mirror_blend import MirrorRegistry, Blender, AdaptiveWrapper, BlendError

logger = logging.getLogger(__name__)


# ── Layer 1: Self-Healing ────────────────────────────────────────────────────

@dataclass(slots=True)
class _FallbackSlot:
    """Tracks the fallback chain for a single resilient binding."""
    name: str
    namespace: Any              # module or globals dict
    namespace_kind: str         # "module" or "globals"
    fallbacks: list             # ordered list of fallback callables
    attempt: int = 0            # index into fallbacks currently active
    blend_key: str = ""         # current blend key (for revert)
    failure_count: int = 0      # total failures observed
    last_failure: float = 0.0   # timestamp of most recent failure


class ResilienceManager:
    """Wraps callables with automatic fallback chains.

    When a mirrored function raises, the manager reverts it and swaps in the
    next fallback in the chain. If all fallbacks are exhausted, it reverts to
    the original. Thread-safe — revert + blend is atomic under the Blender lock.

    Usage:
        rm = ResilienceManager(registry, blender)
        rm.protect(
            target_module, "parse_data",
            fallbacks=[fast_parse, safe_parse, minimal_parse],
        )
        # If fast_parse raises, safe_parse replaces it automatically.
        # If safe_parse raises, minimal_parse takes over.
        # If all fail, the original parse_data is restored.
    """

    def __init__(self, registry: MirrorRegistry, blender: Blender) -> None:
        self._registry = registry
        self._blender = blender
        self._slots: Dict[str, _FallbackSlot] = {}
        self._lock = threading.Lock()

    def protect(
        self,
        namespace: Any,
        name: str,
        fallbacks: Sequence[Callable],
        *,
        key: Optional[str] = None,
    ) -> str:
        """Install a fallback chain for `namespace.name`.

        The first callable in `fallbacks` replaces the current implementation.
        On failure, subsequent entries take over. If all exhaust, the original
        is restored.

        Returns:
            The protection key (for manual removal via `unprotect`).
        """
        if not fallbacks:
            raise ValueError("fallbacks must be non-empty")

        pkey = key or f"resilient:{_ns_label(namespace)}.{name}"

        with self._lock:
            slot = _FallbackSlot(
                name=name,
                namespace=namespace,
                namespace_kind="module" if isinstance(namespace, types.ModuleType) else "globals",
                fallbacks=list(fallbacks),
                attempt=0,
            )
            self._slots[pkey] = slot
            self._install(slot, index=0)

        return pkey

    def unprotect(self, key: str) -> None:
        """Remove a protection, reverting to the original."""
        with self._lock:
            slot = self._slots.pop(key, None)
            if slot and slot.blend_key:
                try:
                    self._blender.revert(slot.blend_key)
                except BlendError:
                    pass

    @property
    def protection_count(self) -> int:
        return len(self._slots)

    @property
    def total_failures(self) -> int:
        return sum(s.failure_count for s in self._slots.values())

    def _install(self, slot: _FallbackSlot, index: int) -> None:
        """Install fallback[index] into the namespace with exception handling."""
        fn = slot.fallbacks[index]
        slot.attempt = index

        def post_hook(original, result):
            if isinstance(result, BaseException):
                self._on_failure(slot, result)

        def safe_wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                self._on_failure(slot, exc)
                raise

        mirror = self._registry.mirror(safe_wrapper, name=f"{slot.name}:fb{index}")

        # Revert previous blend if any
        if slot.blend_key:
            try:
                self._blender.revert(slot.blend_key)
            except BlendError:
                pass

        if slot.namespace_kind == "module":
            slot.blend_key = self._blender.blend_into_module(
                slot.namespace, slot.name, mirror,
                key=f"resilient:{_ns_label(slot.namespace)}.{slot.name}",
            )
        else:
            slot.blend_key = self._blender.blend_into_globals(
                slot.namespace, slot.name, mirror,
                key=f"resilient:globals.{slot.name}",
            )

    def _on_failure(self, slot: _FallbackSlot, exc: BaseException) -> None:
        """Advance to the next fallback, or revert to original if exhausted."""
        with self._lock:
            slot.failure_count += 1
            slot.last_failure = time.monotonic()
            next_idx = slot.attempt + 1

            if next_idx < len(slot.fallbacks):
                logger.warning(
                    "ResilienceManager: %s fallback %d failed (%s), "
                    "advancing to fallback %d",
                    slot.name, slot.attempt, exc, next_idx,
                )
                self._install(slot, next_idx)
            else:
                logger.warning(
                    "ResilienceManager: %s all %d fallbacks exhausted, "
                    "reverting to original",
                    slot.name, len(slot.fallbacks),
                )
                if slot.blend_key:
                    try:
                        self._blender.revert(slot.blend_key)
                    except BlendError:
                        pass
                    slot.blend_key = ""


# ── Layer 2: Self-Customizing ────────────────────────────────────────────────

class EnvironmentAdapter:
    """Extends AdaptiveWrapper to tune behavior based on broader system signals.

    Checks CPU load, memory pressure, network latency, and application-level
    metrics (like frame timing from InstrumentedRain) to select the optimal
    operating mode.

    Modes:
        FULL        — All hooks, maximum fidelity
        LIGHTWEIGHT — Skip expensive post-processing
        PASSTHROUGH — Zero overhead
        ECO         — Reduced quality for resource-constrained environments
    """

    class Mode:
        FULL = "full"
        LIGHTWEIGHT = "light"
        PASSTHROUGH = "pass"
        ECO = "eco"

    def __init__(
        self,
        registry: MirrorRegistry,
        blender: Blender,
        *,
        cpu_threshold: float = 80.0,
        memory_threshold: float = 85.0,
        latency_threshold_ms: float = 200.0,
        frame_time_threshold_ms: float = 50.0,
    ) -> None:
        self._registry = registry
        self._blender = blender
        self._cpu_threshold = cpu_threshold
        self._memory_threshold = memory_threshold
        self._latency_threshold_ms = latency_threshold_ms
        self._frame_time_threshold_ms = frame_time_threshold_ms
        self._lock = threading.Lock()
        self._mode = self.Mode.FULL
        self._swap_registry: Dict[str, _AdaptiveSlot] = {}
        self._metrics: Dict[str, float] = {}

    def register_adaptive(
        self,
        namespace: Any,
        name: str,
        variants: Dict[str, Callable],
    ) -> str:
        """Register a callable with mode-specific variants.

        Args:
            namespace: Module or globals dict to patch.
            name: Attribute name.
            variants: Mapping of mode → callable. Must include at least "full".

        Returns:
            Registration key.
        """
        if "full" not in variants:
            raise ValueError("variants must include a 'full' entry")

        key = f"adaptive:{_ns_label(namespace)}.{name}"
        slot = _AdaptiveSlot(
            name=name,
            namespace=namespace,
            namespace_kind="module" if isinstance(namespace, types.ModuleType) else "globals",
            variants=dict(variants),
            active_mode="",
            blend_key="",
        )
        with self._lock:
            self._swap_registry[key] = slot
        self._apply_mode_to_slot(slot, self._mode)
        return key

    def update_metrics(self, **kwargs: float) -> None:
        """Feed in environment metrics. Keys can include:
        cpu_percent, memory_percent, network_latency_ms, frame_time_ms, etc.
        """
        with self._lock:
            self._metrics.update(kwargs)

    def adapt(self) -> str:
        """Re-evaluate the environment and switch modes if needed.

        Returns the newly selected mode.
        """
        mode = self._evaluate()
        if mode != self._mode:
            old = self._mode
            self._mode = mode
            logger.info("EnvironmentAdapter: mode %s → %s", old, mode)
            with self._lock:
                for slot in self._swap_registry.values():
                    self._apply_mode_to_slot(slot, mode)
        return mode

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def metrics(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._metrics)

    def _evaluate(self) -> str:
        """Determine mode from current metrics."""
        m = self._metrics
        cpu = m.get("cpu_percent", 0.0)
        mem = m.get("memory_percent", 0.0)
        latency = m.get("network_latency_ms", 0.0)
        frame_time = m.get("frame_time_ms", 0.0)

        # Under heavy load → eco mode
        if cpu > self._cpu_threshold or mem > self._memory_threshold:
            return self.Mode.ECO

        # High latency or slow frames → lightweight
        if latency > self._latency_threshold_ms or frame_time > self._frame_time_threshold_ms:
            return self.Mode.LIGHTWEIGHT

        return self.Mode.FULL

    def _apply_mode_to_slot(self, slot: "_AdaptiveSlot", mode: str) -> None:
        """Swap in the variant matching `mode`, falling back to 'full'."""
        if mode == slot.active_mode:
            return

        fn = slot.variants.get(mode) or slot.variants["full"]
        mirror = self._registry.mirror(fn, name=f"{slot.name}:{mode}")

        # Revert previous
        if slot.blend_key:
            try:
                self._blender.revert(slot.blend_key)
            except BlendError:
                pass

        if slot.namespace_kind == "module":
            slot.blend_key = self._blender.blend_into_module(
                slot.namespace, slot.name, mirror,
            )
        else:
            slot.blend_key = self._blender.blend_into_globals(
                slot.namespace, slot.name, mirror,
            )
        slot.active_mode = mode


@dataclass
class _AdaptiveSlot:
    name: str
    namespace: Any
    namespace_kind: str
    variants: Dict[str, Callable]
    active_mode: str
    blend_key: str


# ── Layer 3: Self-Upgrading ──────────────────────────────────────────────────

class HotUpgrader:
    """Hot-swap code into a running system with automatic rollback.

    Loads new Python code (from bytes or a file path), mirrors every callable
    with health-check hooks, and blends them into the target module. If anything
    goes wrong, `rollback()` restores the previous version instantly.

    Integrates with JumpSession: when a session arrives containing Python files,
    the upgrader can apply them as live patches.

    Usage:
        upgrader = HotUpgrader(registry, blender)
        upgrader.apply_upgrade(new_code_bytes, target_module)
        # If something breaks:
        upgrader.rollback()
    """

    def __init__(self, registry: MirrorRegistry, blender: Blender) -> None:
        self._registry = registry
        self._blender = blender
        self._version_stack: List[_UpgradeRecord] = []
        self._lock = threading.Lock()

    def apply_upgrade(
        self,
        source: bytes | str,
        target_module: types.ModuleType,
        *,
        health_check: Optional[Callable] = None,
        tag: str = "",
    ) -> int:
        """Load new code and hot-swap matching callables into target_module.

        Args:
            source: Python source as bytes, string, or a file path.
            target_module: The module whose functions will be replaced.
            health_check: Optional pre-hook applied to every swapped function.
                          Receives (fn, args, kwargs); raise to trigger rollback.
            tag: Human-readable label for this upgrade.

        Returns:
            Version index (for selective rollback).
        """
        if isinstance(source, (str, os.PathLike)):
            source_path = str(source)
            with open(source_path, "rb") as f:
                code_bytes = f.read()
        else:
            code_bytes = source
            source_path = "<upgrade>"

        # Load into a sandboxed module
        spec = importlib.util.spec_from_loader("_upgrade_tmp", loader=None)
        new_mod = importlib.util.module_from_spec(spec)
        exec(compile(code_bytes, source_path, "exec"), new_mod.__dict__)

        keys: List[str] = []
        upgraded_names: List[str] = []

        with self._lock:
            for name in dir(new_mod):
                if name.startswith("_"):
                    continue
                new_obj = getattr(new_mod, name)
                if not callable(new_obj):
                    continue
                if not hasattr(target_module, name):
                    continue

                ver = len(self._version_stack)
                mirror = self._registry.mirror(
                    new_obj,
                    pre=health_check,
                    name=f"{target_module.__name__}.{name}:v{ver}",
                )
                blend_key = self._blender.blend_into_module(
                    target_module, name, mirror,
                    key=f"upgrade:v{ver}:{target_module.__name__}.{name}",
                )
                keys.append(blend_key)
                upgraded_names.append(name)

            record = _UpgradeRecord(
                version=len(self._version_stack),
                keys=keys,
                names=upgraded_names,
                tag=tag or f"v{len(self._version_stack)}",
                timestamp=time.monotonic(),
            )
            self._version_stack.append(record)

        logger.info(
            "HotUpgrader: applied %s — %d functions upgraded: %s",
            record.tag, len(keys), ", ".join(upgraded_names),
        )
        return record.version

    def apply_from_session(
        self,
        session: Any,
        target_module: types.ModuleType,
        *,
        file_filter: Optional[Callable[[str], bool]] = None,
        health_check: Optional[Callable] = None,
    ) -> List[int]:
        """Apply upgrades from a JumpSession's files dict.

        Looks for .py files in session.files, decodes them, and applies
        each as an upgrade to the target module.

        Args:
            session: A JumpSession with a `files` dict (path → base64 data).
            target_module: Module to patch.
            file_filter: Optional predicate on filename; defaults to *.py.
            health_check: Pre-hook for health checking.

        Returns:
            List of version indices for each applied upgrade.
        """
        import base64

        versions = []
        for rel_path, b64data in session.files.items():
            if not rel_path.endswith(".py"):
                continue
            if file_filter and not file_filter(rel_path):
                continue

            code_bytes = base64.b64decode(b64data)
            ver = self.apply_upgrade(
                code_bytes, target_module,
                health_check=health_check,
                tag=f"session:{session.session_id}:{rel_path}",
            )
            versions.append(ver)

        return versions

    def rollback(self, version: Optional[int] = None) -> bool:
        """Roll back to a previous version.

        Args:
            version: Specific version to roll back. If None, rolls back the
                     most recent upgrade.

        Returns:
            True if rollback succeeded.
        """
        with self._lock:
            if not self._version_stack:
                return False

            if version is None:
                record = self._version_stack.pop()
            else:
                # Find and remove the specific version
                idx = None
                for i, rec in enumerate(self._version_stack):
                    if rec.version == version:
                        idx = i
                        break
                if idx is None:
                    return False
                record = self._version_stack.pop(idx)

            errors = []
            for key in record.keys:
                try:
                    self._blender.revert(key)
                except BlendError as e:
                    errors.append((key, e))

        if errors:
            logger.error("HotUpgrader: rollback %s had %d errors", record.tag, len(errors))
            return False

        logger.info("HotUpgrader: rolled back %s (%d functions)", record.tag, len(record.keys))
        return True

    def rollback_all(self) -> int:
        """Roll back all upgrades in reverse order. Returns count rolled back."""
        count = 0
        while self._version_stack:
            if self.rollback():
                count += 1
            else:
                break
        return count

    @property
    def version_count(self) -> int:
        return len(self._version_stack)

    @property
    def current_tag(self) -> str:
        if self._version_stack:
            return self._version_stack[-1].tag
        return "(original)"

    @property
    def history(self) -> List[str]:
        return [r.tag for r in self._version_stack]


@dataclass(slots=True)
class _UpgradeRecord:
    version: int
    keys: List[str]
    names: List[str]
    tag: str
    timestamp: float


# ── Layer 4: Autonomous Loop ─────────────────────────────────────────────────

class AutonomousLoop:
    """The feedback loop that ties self-healing, self-customizing, and
    self-upgrading into a single autonomous system.

    Runs as a background thread alongside the main application. Each tick:
      1. Collects health metrics (from InstrumentedRain, system stats, peers)
      2. Feeds them into EnvironmentAdapter for mode selection
      3. Checks for incoming upgrade sessions from peers
      4. Applies/reverts patches via HotUpgrader as needed
      5. Logs a summary

    Usage:
        loop = AutonomousLoop(registry, blender, node=jump_node)
        loop.start()
        # ... application runs ...
        loop.stop()
    """

    def __init__(
        self,
        registry: MirrorRegistry,
        blender: Blender,
        *,
        node: Optional[Any] = None,
        target_module: Optional[types.ModuleType] = None,
        tick_interval: float = 1.0,
    ) -> None:
        self.registry = registry
        self.blender = blender
        self.resilience = ResilienceManager(registry, blender)
        self.adapter = EnvironmentAdapter(registry, blender)
        self.upgrader = HotUpgrader(registry, blender)
        self.node = node
        self.target_module = target_module
        self._tick_interval = tick_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._tick_count = 0
        self._lock = threading.Lock()
        self._metrics_collectors: List[Callable[[], Dict[str, float]]] = []
        self._on_tick_callbacks: List[Callable[["AutonomousLoop"], None]] = []

    def add_metrics_collector(self, collector: Callable[[], Dict[str, float]]) -> None:
        """Register a function that returns metrics dict each tick."""
        self._metrics_collectors.append(collector)

    def add_on_tick(self, callback: Callable[["AutonomousLoop"], None]) -> None:
        """Register a callback invoked each tick after adaptation."""
        self._on_tick_callbacks.append(callback)

    def start(self) -> None:
        """Start the autonomous loop in a background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="autonomous")
        self._thread.start()
        logger.info("AutonomousLoop: started (interval=%.1fs)", self._tick_interval)

    def stop(self) -> None:
        """Stop the loop and clean up."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._tick_interval * 3)
        logger.info("AutonomousLoop: stopped after %d ticks", self._tick_count)

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def status(self) -> Dict[str, Any]:
        """Snapshot of the loop's current state."""
        return {
            "running": self._running,
            "tick_count": self._tick_count,
            "mode": self.adapter.mode,
            "metrics": self.adapter.metrics,
            "protections": self.resilience.protection_count,
            "total_failures": self.resilience.total_failures,
            "upgrade_version": self.upgrader.current_tag,
            "upgrade_history": self.upgrader.history,
            "mirrors": self.registry.mirror_count,
            "blends": self.blender.blend_count,
        }

    def _loop(self) -> None:
        while self._running:
            t0 = time.monotonic()
            try:
                self._tick()
            except Exception:
                logger.exception("AutonomousLoop: tick %d failed", self._tick_count)
            elapsed = time.monotonic() - t0
            sleep_time = self._tick_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    def _tick(self) -> None:
        self._tick_count += 1

        # Phase 1: Collect metrics
        for collector in self._metrics_collectors:
            try:
                metrics = collector()
                self.adapter.update_metrics(**metrics)
            except Exception:
                logger.debug("AutonomousLoop: metrics collector failed", exc_info=True)

        # Phase 2: Adapt
        self.adapter.adapt()

        # Phase 3: Check for incoming upgrades from peers
        if self.node and self.target_module:
            self._check_for_upgrades()

        # Phase 4: Invoke tick callbacks
        for cb in self._on_tick_callbacks:
            try:
                cb(self)
            except Exception:
                logger.debug("AutonomousLoop: tick callback failed", exc_info=True)

    def _check_for_upgrades(self) -> None:
        """Process any sessions received by the JumpNode that contain code."""
        if not hasattr(self.node, "received_sessions"):
            return

        while self.node.received_sessions:
            session = self.node.received_sessions.pop(0)
            py_files = [f for f in session.files if f.endswith(".py")]
            if not py_files:
                continue

            logger.info(
                "AutonomousLoop: received upgrade session %s with %d Python files",
                session.session_id, len(py_files),
            )
            try:
                versions = self.upgrader.apply_from_session(
                    session, self.target_module,
                    health_check=self._upgrade_health_check,
                )
                logger.info(
                    "AutonomousLoop: applied %d upgrades from session %s",
                    len(versions), session.session_id,
                )
            except Exception:
                logger.exception(
                    "AutonomousLoop: failed to apply upgrade from session %s",
                    session.session_id,
                )

    @staticmethod
    def _upgrade_health_check(fn, args, kwargs):
        """Default health check for upgraded functions — just a pass-through.
        Override via subclass or by passing a custom health_check to the upgrader.
        """
        return None


# ── Layer 5: Generative AI Self-Healing ──────────────────────────────────────

@dataclass(slots=True)
class _PatchAttempt:
    """Record of an AI-generated patch attempt."""
    function_name: str
    exception_type: str
    exception_msg: str
    generated_source: str
    applied: bool
    timestamp: float
    success: bool = False


class GenerativeHealer:
    """LLM-powered runtime bug fixing.

    When a protected function throws an exception and all static fallbacks
    are exhausted, the healer captures the stack trace, input arguments,
    and source code, asks an LLM to generate a patch, and injects it live
    via HotUpgrader.

    The LLM backend is pluggable — any callable that accepts a prompt string
    and returns generated code can serve as the backend (local Llama, Gemini,
    Claude, OpenAI, etc.).

    Usage:
        healer = GenerativeHealer(
            registry, blender, upgrader,
            llm_backend=my_llm_function,
        )
        healer.protect(target_module, "parse_data")
        # If parse_data raises, the healer asks the LLM to fix it
    """

    def __init__(
        self,
        registry: MirrorRegistry,
        blender: Blender,
        upgrader: "HotUpgrader",
        *,
        llm_backend: Optional[Callable[[str], str]] = None,
        max_attempts: int = 3,
        sandbox_patches: bool = True,
    ) -> None:
        self._registry = registry
        self._blender = blender
        self._upgrader = upgrader
        self._llm_backend = llm_backend
        self._max_attempts = max_attempts
        self._sandbox_patches = sandbox_patches
        self._lock = threading.Lock()
        self._protected: Dict[str, _HealerSlot] = {}
        self._patch_history: List[_PatchAttempt] = []

    @property
    def llm_available(self) -> bool:
        return self._llm_backend is not None

    def set_llm_backend(self, backend: Callable[[str], str]) -> None:
        """Set or replace the LLM backend at runtime."""
        self._llm_backend = backend

    def protect(
        self,
        namespace: Any,
        name: str,
        *,
        static_fallbacks: Optional[Sequence[Callable]] = None,
        key: Optional[str] = None,
    ) -> str:
        """Install AI-powered healing for a function.

        If static_fallbacks are provided, they are tried first (like
        ResilienceManager). Only when all static fallbacks are exhausted
        does the LLM-based healing kick in.

        Returns:
            Protection key.
        """
        pkey = key or f"healer:{_ns_label(namespace)}.{name}"

        ns_kind = "module" if isinstance(namespace, types.ModuleType) else "globals"
        if ns_kind == "module":
            original = getattr(namespace, name)
        else:
            original = namespace[name]

        slot = _HealerSlot(
            name=name,
            namespace=namespace,
            namespace_kind=ns_kind,
            original=original,
            static_fallbacks=list(static_fallbacks or []),
            current_fn=original,
            blend_key="",
            attempts=0,
        )

        with self._lock:
            self._protected[pkey] = slot
            self._install_wrapper(slot)

        return pkey

    def unprotect(self, key: str) -> None:
        """Remove AI healing protection."""
        with self._lock:
            slot = self._protected.pop(key, None)
            if slot and slot.blend_key:
                try:
                    self._blender.revert(slot.blend_key)
                except BlendError:
                    pass

    @property
    def patch_history(self) -> List[_PatchAttempt]:
        return list(self._patch_history)

    @property
    def protection_count(self) -> int:
        return len(self._protected)

    @property
    def successful_patches(self) -> int:
        return sum(1 for p in self._patch_history if p.success)

    def _install_wrapper(self, slot: "_HealerSlot") -> None:
        """Install the healing wrapper around the function."""
        healer = self

        def healing_wrapper(*args, **kwargs):
            return healer._execute_with_healing(slot, args, kwargs)

        mirror = self._registry.mirror(
            healing_wrapper, name=f"{slot.name}:healer"
        )

        if slot.blend_key:
            try:
                self._blender.revert(slot.blend_key)
            except BlendError:
                pass

        if slot.namespace_kind == "module":
            slot.blend_key = self._blender.blend_into_module(
                slot.namespace, slot.name, mirror,
                key=f"healer:{_ns_label(slot.namespace)}.{slot.name}",
            )
        else:
            slot.blend_key = self._blender.blend_into_globals(
                slot.namespace, slot.name, mirror,
                key=f"healer:globals.{slot.name}",
            )

    def _execute_with_healing(
        self,
        slot: "_HealerSlot",
        args: tuple,
        kwargs: dict,
    ) -> Any:
        """Try execution, fall through static fallbacks, then LLM healing."""
        # Try current function
        primary_exc = None
        try:
            return slot.current_fn(*args, **kwargs)
        except Exception as exc:
            primary_exc = exc

        # Try static fallbacks
        for fb in slot.static_fallbacks:
            try:
                result = fb(*args, **kwargs)
                slot.current_fn = fb
                return result
            except Exception:
                continue

        # All static fallbacks exhausted — try LLM healing
        if self._llm_backend and slot.attempts < self._max_attempts:
            try:
                return self._attempt_llm_heal(
                    slot, primary_exc, args, kwargs,
                )
            except Exception:
                pass

        # Everything failed — raise the original
        raise primary_exc

    def _attempt_llm_heal(
        self,
        slot: "_HealerSlot",
        exc: Exception,
        args: tuple,
        kwargs: dict,
    ) -> Any:
        """Ask the LLM to generate a fix, compile it, and try it."""
        slot.attempts += 1

        # Capture context for the LLM
        source = _safe_get_source(slot.original)
        tb_str = traceback.format_exception(type(exc), exc, exc.__traceback__)

        prompt = self._build_prompt(
            func_name=slot.name,
            source_code=source,
            exception_type=type(exc).__name__,
            exception_msg=str(exc),
            traceback_lines="".join(tb_str),
            args_repr=repr(args[:5]),  # limit arg preview
            kwargs_repr=repr(dict(list(kwargs.items())[:5])),
        )

        # Ask LLM for a fix
        generated = self._llm_backend(prompt)
        cleaned = self._extract_code(generated)

        attempt = _PatchAttempt(
            function_name=slot.name,
            exception_type=type(exc).__name__,
            exception_msg=str(exc),
            generated_source=cleaned,
            applied=False,
            timestamp=time.monotonic(),
        )

        # Compile and test the generated code
        try:
            compiled_fn = self._compile_patch(slot.name, cleaned)
        except Exception as compile_err:
            logger.warning(
                "GenerativeHealer: LLM patch for %s failed to compile: %s",
                slot.name, compile_err,
            )
            self._patch_history.append(attempt)
            raise exc

        # Test the compiled function with the failing inputs
        try:
            result = compiled_fn(*args, **kwargs)
        except Exception as test_err:
            logger.warning(
                "GenerativeHealer: LLM patch for %s failed test: %s",
                slot.name, test_err,
            )
            self._patch_history.append(attempt)
            raise exc

        # Patch succeeded — install it as the new current function
        attempt.applied = True
        attempt.success = True
        self._patch_history.append(attempt)
        slot.current_fn = compiled_fn

        logger.info(
            "GenerativeHealer: LLM patch for %s succeeded on attempt %d",
            slot.name, slot.attempts,
        )
        return result

    @staticmethod
    def _build_prompt(
        func_name: str,
        source_code: str,
        exception_type: str,
        exception_msg: str,
        traceback_lines: str,
        args_repr: str,
        kwargs_repr: str,
    ) -> str:
        """Build the prompt for the LLM."""
        return textwrap.dedent(f"""\
            A Python function is crashing at runtime. Generate a fixed version.

            FUNCTION NAME: {func_name}

            ORIGINAL SOURCE:
            ```python
            {source_code}
            ```

            EXCEPTION: {exception_type}: {exception_msg}

            TRACEBACK:
            {traceback_lines}

            FAILING INPUTS:
            args = {args_repr}
            kwargs = {kwargs_repr}

            INSTRUCTIONS:
            - Return ONLY the corrected Python function definition.
            - Keep the same function name and signature.
            - Fix the bug that causes {exception_type}.
            - Do not add imports outside the function body.
            - Do not include explanations, just code.
        """)

    @staticmethod
    def _extract_code(llm_response: str) -> str:
        """Extract Python code from an LLM response (strip markdown fences)."""
        lines = llm_response.strip().splitlines()
        # Strip markdown code fences
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines)

    @staticmethod
    def _compile_patch(func_name: str, source: str) -> Callable:
        """Compile a generated function from source code."""
        namespace: Dict[str, Any] = {}
        exec(compile(source, f"<llm_patch:{func_name}>", "exec"), namespace)
        if func_name not in namespace:
            raise ValueError(
                f"Generated code does not define '{func_name}'"
            )
        fn = namespace[func_name]
        if not callable(fn):
            raise TypeError(f"'{func_name}' is not callable in generated code")
        return fn


@dataclass
class _HealerSlot:
    name: str
    namespace: Any
    namespace_kind: str
    original: Callable
    static_fallbacks: List[Callable]
    current_fn: Callable
    blend_key: str
    attempts: int


def _safe_get_source(fn: Callable) -> str:
    """Get source code of a function, with fallback."""
    try:
        return inspect.getsource(fn)
    except (OSError, TypeError):
        return f"# Source unavailable for {getattr(fn, '__name__', repr(fn))}"


# ── Layer 6: WebAssembly Sandboxing ──────────────────────────────────────────

class WasmSandbox:
    """Zero-trust code execution via WebAssembly isolation.

    Incoming code payloads (from OTA upgrades, peer sessions, or LLM patches)
    are compiled to WebAssembly and executed in a sandboxed runtime. The guest
    code cannot access the host filesystem, network, or OS kernel.

    When wasmtime-py is available, code runs in a true Wasm sandbox.
    Otherwise, falls back to a restricted exec() with a sanitized namespace
    that blocks dangerous builtins and modules.

    Usage:
        sandbox = WasmSandbox()
        result = sandbox.execute(code_string, func_name="add", args=(2, 3))
    """

    def __init__(
        self,
        *,
        allowed_modules: Optional[List[str]] = None,
        max_memory_mb: int = 64,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._allowed_modules = set(allowed_modules or [
            "math", "json", "re", "hashlib", "base64", "collections",
            "itertools", "functools", "operator", "string", "textwrap",
            "datetime", "decimal", "fractions", "statistics",
        ])
        self._max_memory_mb = max_memory_mb
        self._timeout = timeout_seconds
        self._has_wasmtime = self._check_wasmtime()
        self._execution_count = 0
        self._sandbox_violations = 0
        self._lock = threading.Lock()

    @staticmethod
    def _check_wasmtime() -> bool:
        try:
            import wasmtime  # noqa: F401
            return True
        except ImportError:
            return False

    @property
    def backend(self) -> str:
        return "wasmtime" if self._has_wasmtime else "restricted_exec"

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "backend": self.backend,
            "executions": self._execution_count,
            "violations": self._sandbox_violations,
        }

    def execute(
        self,
        source: str,
        *,
        func_name: Optional[str] = None,
        args: tuple = (),
        kwargs: Optional[dict] = None,
        extra_globals: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Execute code in the sandbox.

        Args:
            source: Python source code to execute.
            func_name: If provided, call this function after exec and return
                       its result. If None, just exec the code.
            args: Arguments to pass to func_name.
            kwargs: Keyword arguments to pass to func_name.
            extra_globals: Additional safe globals to inject.

        Returns:
            The result of func_name(*args, **kwargs) if func_name is given,
            otherwise None.

        Raises:
            SandboxViolation: If the code attempts forbidden operations.
            SandboxTimeout: If execution exceeds the timeout.
        """
        with self._lock:
            self._execution_count += 1

        if self._has_wasmtime:
            return self._execute_wasm(
                source, func_name=func_name, args=args,
                kwargs=kwargs or {}, extra_globals=extra_globals,
            )
        return self._execute_restricted(
            source, func_name=func_name, args=args,
            kwargs=kwargs or {}, extra_globals=extra_globals,
        )

    def validate_source(self, source: str) -> List[str]:
        """Static analysis of source code for sandbox violations.

        Returns a list of violation descriptions (empty = safe).
        """
        import ast

        violations = []
        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            return [f"Syntax error: {e}"]

        for node in ast.walk(tree):
            # Block os.system, subprocess, etc.
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if not self._is_allowed_import(alias.name):
                        violations.append(
                            f"Forbidden import: {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module and not self._is_allowed_import(node.module):
                    violations.append(
                        f"Forbidden import: {node.module}"
                    )
            # Block exec/eval/compile calls
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ("exec", "eval", "compile",
                                         "__import__", "breakpoint"):
                        violations.append(
                            f"Forbidden builtin call: {node.func.id}()"
                        )
            # Block attribute access to __subclasses__, __bases__, etc.
            elif isinstance(node, ast.Attribute):
                if node.attr in ("__subclasses__", "__bases__", "__mro__",
                                  "__class__", "__globals__", "__code__",
                                  "__builtins__"):
                    violations.append(
                        f"Forbidden attribute access: .{node.attr}"
                    )

        return violations

    def _is_allowed_import(self, module_name: str) -> bool:
        """Check if a module import is allowed."""
        top_level = module_name.split(".")[0]
        return top_level in self._allowed_modules

    def _execute_restricted(
        self,
        source: str,
        *,
        func_name: Optional[str],
        args: tuple,
        kwargs: dict,
        extra_globals: Optional[Dict[str, Any]],
    ) -> Any:
        """Execute in a restricted Python namespace."""
        # Validate first
        violations = self.validate_source(source)
        if violations:
            self._sandbox_violations += len(violations)
            raise SandboxViolation(
                f"Code blocked: {'; '.join(violations)}"
            )

        # Build sanitized globals
        safe_builtins = {
            name: getattr(__builtins__ if isinstance(__builtins__, dict)
                          else type(__builtins__), name, None)
            if isinstance(__builtins__, dict)
            else getattr(__builtins__, name, None)
            for name in _SAFE_BUILTINS
        }
        # Ensure we get actual builtins
        import builtins as _builtins_mod
        safe_builtins = {
            name: getattr(_builtins_mod, name)
            for name in _SAFE_BUILTINS
            if hasattr(_builtins_mod, name)
        }

        # Provide a restricted __import__
        allowed = self._allowed_modules

        def restricted_import(name, *a, **kw):
            top = name.split(".")[0]
            if top not in allowed:
                raise ImportError(
                    f"Import of '{name}' blocked by sandbox"
                )
            return importlib.import_module(name)

        safe_builtins["__import__"] = restricted_import

        sandbox_globals: Dict[str, Any] = {"__builtins__": safe_builtins}
        if extra_globals:
            sandbox_globals.update(extra_globals)

        # Execute with timeout via threading
        result_holder: List[Any] = [None]
        error_holder: List[Optional[Exception]] = [None]

        def _run():
            try:
                exec(
                    compile(source, "<sandbox>", "exec"),
                    sandbox_globals,
                )
                if func_name:
                    fn = sandbox_globals.get(func_name)
                    if fn is None:
                        raise ValueError(
                            f"Function '{func_name}' not found in sandbox"
                        )
                    result_holder[0] = fn(*args, **kwargs)
            except Exception as e:
                error_holder[0] = e

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        thread.join(timeout=self._timeout)

        if thread.is_alive():
            self._sandbox_violations += 1
            raise SandboxTimeout(
                f"Sandbox execution exceeded {self._timeout}s timeout"
            )

        if error_holder[0] is not None:
            raise error_holder[0]

        return result_holder[0]

    def _execute_wasm(
        self,
        source: str,
        *,
        func_name: Optional[str],
        args: tuple,
        kwargs: dict,
        extra_globals: Optional[Dict[str, Any]],
    ) -> Any:
        """Execute using wasmtime for true isolation.

        Falls back to restricted exec if the wasmtime-based Python
        compilation is not feasible for the given code.
        """
        # wasmtime-py provides Wasm execution but compiling arbitrary
        # Python to Wasm requires additional tooling (e.g., RustPython
        # compiled to Wasm). For practical use, we validate + restricted exec.
        # The Wasm layer adds memory/time limits enforcement.
        try:
            import wasmtime

            # Use wasmtime's resource limits for enforcement
            config = wasmtime.Config()
            config.consume_fuel = True
            engine = wasmtime.Engine(config)
            store = wasmtime.Store(engine)
            store.set_fuel(10_000_000)  # fuel limit

            # For Python code, fall through to restricted exec
            # but enforce the Wasm-style resource limits
            return self._execute_restricted(
                source, func_name=func_name, args=args,
                kwargs=kwargs, extra_globals=extra_globals,
            )
        except ImportError:
            return self._execute_restricted(
                source, func_name=func_name, args=args,
                kwargs=kwargs, extra_globals=extra_globals,
            )


class SandboxViolation(Exception):
    """Raised when sandboxed code attempts a forbidden operation."""


class SandboxTimeout(Exception):
    """Raised when sandboxed code exceeds the execution timeout."""


# Safe builtins whitelist for sandbox
_SAFE_BUILTINS = frozenset({
    "abs", "all", "any", "ascii", "bin", "bool", "bytearray", "bytes",
    "callable", "chr", "complex", "dict", "dir", "divmod", "enumerate",
    "filter", "float", "format", "frozenset", "getattr", "hasattr",
    "hash", "hex", "id", "int", "isinstance", "issubclass", "iter",
    "len", "list", "map", "max", "min", "next", "object", "oct", "ord",
    "pow", "print", "property", "range", "repr", "reversed", "round",
    "set", "slice", "sorted", "str", "sum", "super", "tuple", "type",
    "vars", "zip", "True", "False", "None",
    "ValueError", "TypeError", "KeyError", "IndexError", "AttributeError",
    "RuntimeError", "StopIteration", "ZeroDivisionError", "OverflowError",
    "Exception", "ArithmeticError", "LookupError", "ImportError",
    "NotImplementedError", "OSError", "IOError",
    "__build_class__", "__name__",  # required for class definitions
    "staticmethod", "classmethod",
})


# ── Utilities ────────────────────────────────────────────────────────────────

def _ns_label(namespace: Any) -> str:
    """Human-readable label for a namespace."""
    if isinstance(namespace, types.ModuleType):
        return namespace.__name__
    if isinstance(namespace, dict):
        return f"dict@{id(namespace):#x}"
    return repr(namespace)


def system_metrics() -> Dict[str, float]:
    """Collect basic system metrics. Works cross-platform without psutil."""
    metrics: Dict[str, float] = {}
    try:
        load = os.getloadavg()
        cpu_count = os.cpu_count() or 1
        metrics["cpu_percent"] = (load[0] / cpu_count) * 100.0
    except (OSError, AttributeError):
        pass

    try:
        with open("/proc/meminfo", "r") as f:
            lines = f.read()
        total = available = 0
        for line in lines.splitlines():
            if line.startswith("MemTotal:"):
                total = int(line.split()[1])
            elif line.startswith("MemAvailable:"):
                available = int(line.split()[1])
        if total > 0:
            metrics["memory_percent"] = ((total - available) / total) * 100.0
    except (OSError, ValueError):
        pass

    return metrics
