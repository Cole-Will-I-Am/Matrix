"""
Session Jumper — Serialize, transfer, and resume sessions across devices.

A "session" is a bundle of state (environment variables, working directory,
open files, clipboard, arbitrary key-value data) that can be frozen on one
device and thawed on another.

Extended with deep execution-state serialization: freeze live Python objects,
local variables, generator states, and class instances using dill/cloudpickle,
then resume them on a completely different machine.
"""

import gzip
import hashlib
import inspect
import json
import logging
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from device_discovery import Device, Transport, DiscoveryManager
from jump_protocol import (
    JumpConnection, JumpListener, MsgType, ProtocolError,
    client_handshake, CHUNK_SIZE,
)


logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 10 * 1024 * 1024

# ── Deep Serialization Backend ───────────────────────────────────────────────

def _get_serializer():
    """Return the best available deep serializer (dill > cloudpickle > pickle)."""
    try:
        import dill
        return dill
    except ImportError:
        pass
    try:
        import cloudpickle
        return cloudpickle
    except ImportError:
        pass
    import pickle
    return pickle


def _serializer_name() -> str:
    """Return the name of the active serializer backend."""
    s = _get_serializer()
    return s.__name__


# ── Session Model ────────────────────────────────────────────────────────────

@dataclass
class JumpSession:
    """Serializable session state that travels between devices."""
    session_id: str
    source_device: str
    timestamp: float = 0.0
    cwd: str = ""
    env: dict = field(default_factory=dict)
    clipboard: str = ""
    files: dict = field(default_factory=dict)  # relative_path → bytes (base64)
    metadata: dict = field(default_factory=dict)
    checksum: str = ""

    def serialize(self) -> bytes:
        """Serialize to compressed JSON bytes."""
        d = asdict(self)
        d.pop("checksum", None)
        raw = json.dumps(d, sort_keys=True).encode()
        compressed = gzip.compress(raw, compresslevel=6)
        return compressed

    @classmethod
    def deserialize(cls, data: bytes) -> "JumpSession":
        """Deserialize from compressed JSON bytes."""
        raw = gzip.decompress(data)
        d = json.loads(raw.decode())
        return cls(**d)

    def compute_checksum(self) -> str:
        d = asdict(self)
        d.pop("checksum", None)
        raw = json.dumps(d, sort_keys=True).encode()
        return hashlib.sha256(raw).hexdigest()

    def validate(self) -> bool:
        if not self.checksum:
            return True
        return self.checksum == self.compute_checksum()


def capture_session(session_id: str, source_device: str,
                    include_env: bool = True,
                    include_files: list[str] = None,
                    extra_metadata: dict = None) -> JumpSession:
    """Capture the current environment as a JumpSession."""
    import base64

    env = {}
    if include_env:
        # Only capture safe, non-secret env vars
        safe_prefixes = ("HOME", "USER", "SHELL", "LANG", "TERM", "PATH",
                         "PWD", "EDITOR", "VISUAL", "DISPLAY")
        env = {k: v for k, v in os.environ.items()
               if any(k.startswith(p) for p in safe_prefixes)}

    files = {}
    if include_files:
        for fpath in include_files:
            p = Path(fpath)
            if not p.exists():
                logger.warning("Skipping missing file: %s", p)
                continue
            if p.is_file() and p.stat().st_size < MAX_FILE_SIZE:
                files[str(p)] = base64.b64encode(p.read_bytes()).decode()

    session = JumpSession(
        session_id=session_id,
        source_device=source_device,
        timestamp=time.time(),
        cwd=os.getcwd(),
        env=env,
        files=files,
        metadata=extra_metadata or {},
    )
    session.checksum = session.compute_checksum()
    return session


def restore_session(session: JumpSession, restore_env: bool = False,
                    restore_files: bool = False, target_dir: str = None):
    """Apply a received session on this device."""
    import base64

    if not session.validate():
        raise ValueError("Session checksum mismatch — data may be corrupted")

    if restore_env:
        for k, v in session.env.items():
            os.environ[k] = v

    if restore_files and session.files:
        base = Path(target_dir) if target_dir else Path.cwd()
        for rel_path, b64data in session.files.items():
            dest = base / rel_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(base64.b64decode(b64data))

    return session.metadata


# ── Deep Execution State Serialization ────────────────────────────────────────

@dataclass
class ExecutionSnapshot:
    """A frozen snapshot of live Python execution state.

    Captures local variables, object instances, generator states, and
    arbitrary Python objects using dill/cloudpickle for deep serialization.
    Unlike JumpSession (which only captures env vars, cwd, and flat files),
    this captures the *live memory* of a running program.
    """
    snapshot_id: str
    source_device: str
    timestamp: float = 0.0
    local_vars: bytes = b""       # serialized dict of local variables
    objects: bytes = b""          # serialized dict of named objects
    generators: bytes = b""       # serialized generator/coroutine states
    call_stack_info: list = field(default_factory=list)  # frame metadata (non-serializable)
    metadata: dict = field(default_factory=dict)
    serializer_backend: str = ""
    checksum: str = ""

    def compute_checksum(self) -> str:
        h = hashlib.sha256()
        h.update(self.snapshot_id.encode())
        h.update(self.source_device.encode())
        h.update(self.local_vars)
        h.update(self.objects)
        h.update(self.generators)
        return h.hexdigest()

    def validate(self) -> bool:
        if not self.checksum:
            return True
        return self.checksum == self.compute_checksum()

    def serialize(self) -> bytes:
        """Serialize the snapshot to compressed bytes."""
        serializer = _get_serializer()
        payload = {
            "snapshot_id": self.snapshot_id,
            "source_device": self.source_device,
            "timestamp": self.timestamp,
            "local_vars": self.local_vars,
            "objects": self.objects,
            "generators": self.generators,
            "call_stack_info": self.call_stack_info,
            "metadata": self.metadata,
            "serializer_backend": self.serializer_backend,
            "checksum": self.checksum,
        }
        raw = serializer.dumps(payload)
        return gzip.compress(raw, compresslevel=6)

    @classmethod
    def deserialize(cls, data: bytes) -> "ExecutionSnapshot":
        """Deserialize from compressed bytes."""
        serializer = _get_serializer()
        raw = gzip.decompress(data)
        payload = serializer.loads(raw)
        return cls(**payload)


def freeze_execution_state(
    snapshot_id: str,
    source_device: str,
    *,
    local_vars: Optional[Dict[str, Any]] = None,
    objects: Optional[Dict[str, Any]] = None,
    generators: Optional[Dict[str, Any]] = None,
    capture_caller_locals: bool = False,
    extra_metadata: Optional[dict] = None,
) -> ExecutionSnapshot:
    """Freeze live Python execution state into a portable snapshot.

    Args:
        snapshot_id: Unique identifier for this snapshot.
        source_device: Device name/ID.
        local_vars: Dict of variable name → value to serialize.
        objects: Dict of named objects (class instances, data structures).
        generators: Dict of named generator/iterator objects.
        capture_caller_locals: If True, automatically capture the caller's
                               local variables (via frame inspection).
        extra_metadata: Arbitrary metadata dict.

    Returns:
        ExecutionSnapshot ready for transfer via JumpSession or direct send.
    """
    serializer = _get_serializer()

    # Auto-capture caller's locals if requested
    if capture_caller_locals:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            caller_locals = {
                k: v for k, v in frame.f_back.f_locals.items()
                if not k.startswith("__") and _is_serializable(v, serializer)
            }
            if local_vars:
                caller_locals.update(local_vars)
            local_vars = caller_locals

    # Capture call stack metadata (non-serializable frame info)
    stack_info = []
    for fi in inspect.stack()[1:4]:  # up to 3 frames above
        stack_info.append({
            "filename": fi.filename,
            "lineno": fi.lineno,
            "function": fi.function,
            "code_context": (fi.code_context[0].strip()
                             if fi.code_context else ""),
        })

    # Serialize each category
    ser_locals = serializer.dumps(local_vars or {})
    ser_objects = serializer.dumps(objects or {})
    ser_generators = serializer.dumps(generators or {})

    snapshot = ExecutionSnapshot(
        snapshot_id=snapshot_id,
        source_device=source_device,
        timestamp=time.time(),
        local_vars=ser_locals,
        objects=ser_objects,
        generators=ser_generators,
        call_stack_info=stack_info,
        metadata=extra_metadata or {},
        serializer_backend=serializer.__name__,
    )
    snapshot.checksum = snapshot.compute_checksum()
    return snapshot


def thaw_execution_state(
    snapshot: ExecutionSnapshot,
) -> Dict[str, Any]:
    """Restore a frozen execution snapshot, returning the thawed state.

    Returns:
        Dict with keys 'local_vars', 'objects', 'generators', each
        containing the deserialized Python objects ready for use.
    """
    if not snapshot.validate():
        raise ValueError("Snapshot checksum mismatch — data may be corrupted")

    serializer = _get_serializer()

    return {
        "local_vars": serializer.loads(snapshot.local_vars) if snapshot.local_vars else {},
        "objects": serializer.loads(snapshot.objects) if snapshot.objects else {},
        "generators": serializer.loads(snapshot.generators) if snapshot.generators else {},
        "call_stack_info": snapshot.call_stack_info,
        "metadata": snapshot.metadata,
    }


def _is_serializable(obj: Any, serializer) -> bool:
    """Check if an object can be serialized by the given backend."""
    try:
        serializer.dumps(obj)
        return True
    except (TypeError, AttributeError, serializer.PicklingError
            if hasattr(serializer, "PicklingError") else TypeError):
        return False


def embed_snapshot_in_session(
    session: JumpSession,
    snapshot: ExecutionSnapshot,
) -> JumpSession:
    """Embed a deep ExecutionSnapshot into a JumpSession's metadata.

    This allows deep state to piggyback on the existing session transfer
    protocol without changing the wire format.
    """
    import base64
    session.metadata["__deep_snapshot__"] = base64.b64encode(
        snapshot.serialize()
    ).decode()
    session.metadata["__snapshot_id__"] = snapshot.snapshot_id
    session.checksum = session.compute_checksum()
    return session


def extract_snapshot_from_session(
    session: JumpSession,
) -> Optional[ExecutionSnapshot]:
    """Extract a deep ExecutionSnapshot from a JumpSession, if present."""
    import base64
    encoded = session.metadata.get("__deep_snapshot__")
    if not encoded:
        return None
    raw = base64.b64decode(encoded)
    return ExecutionSnapshot.deserialize(raw)


# ── Jump Sender / Receiver ───────────────────────────────────────────────────

def send_session(conn: JumpConnection, session: JumpSession) -> bool:
    """Send a session over an established JumpConnection."""
    data = session.serialize()
    meta = {
        "session_id": session.session_id,
        "source": session.source_device,
        "size": len(data),
        "checksum": session.checksum,
        "timestamp": session.timestamp,
    }
    conn.send_json(MsgType.SESSION_DATA, {"meta": meta, "stage": "meta"})

    # Wait for ready signal
    msg_type, resp = conn.recv_json()
    if msg_type == MsgType.ERROR:
        raise ProtocolError(f"Receiver rejected session: {resp}")

    # Send data in chunks
    offset = 0
    seq = 0
    while offset < len(data):
        chunk = data[offset:offset + CHUNK_SIZE]
        chunk_meta = {"seq": seq, "offset": offset, "size": len(chunk),
                      "final": offset + len(chunk) >= len(data)}
        payload = json.dumps(chunk_meta).encode() + b"\x00" + chunk
        conn.send(MsgType.FILE_CHUNK, payload)
        offset += len(chunk)
        seq += 1

    # Wait for final ACK
    msg_type, ack = conn.recv_json()
    if msg_type != MsgType.SESSION_ACK:
        raise ProtocolError(f"Expected SESSION_ACK, got {msg_type}")
    return ack.get("status") == "ok"


def receive_session(conn: JumpConnection) -> JumpSession:
    """Receive a session over an established JumpConnection."""
    # Get metadata
    msg_type, info = conn.recv_json()
    if msg_type != MsgType.SESSION_DATA:
        raise ProtocolError(f"Expected SESSION_DATA, got {msg_type}")

    meta = info["meta"]
    expected_size = meta["size"]

    # Signal ready
    conn.send_json(MsgType.SESSION_ACK, {"status": "ready"})

    # Receive chunks
    buf = bytearray()
    while len(buf) < expected_size:
        msg_type, raw = conn.recv()
        if msg_type != MsgType.FILE_CHUNK:
            raise ProtocolError(f"Expected FILE_CHUNK, got {msg_type}")
        sep = raw.find(b"\x00")
        if sep == -1:
            raise ValueError("Invalid session data: missing separator")
        chunk_data = raw[sep + 1:]
        buf.extend(chunk_data)

    session = JumpSession.deserialize(bytes(buf))

    if meta.get("checksum") and session.compute_checksum() != meta["checksum"]:
        conn.send_json(MsgType.SESSION_ACK, {"status": "checksum_error"})
        raise ValueError("Session checksum mismatch")

    conn.send_json(MsgType.SESSION_ACK, {"status": "ok"})
    return session


# ── High-level jump operations ───────────────────────────────────────────────

def jump_to_device(target: Device, session: JumpSession,
                   auth_token: str = None, timeout: float = 30.0) -> bool:
    """Jump to a target device: connect, handshake, send session."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((target.address, target.port))
        conn = client_handshake(sock, session.source_device, auth_token)
        return send_session(conn, session)
    except (OSError, ProtocolError, ConnectionError) as e:
        raise JumpError(f"Failed to jump to {target.name}: {e}") from e
    finally:
        try:
            sock.close()
        except OSError:
            pass


class JumpError(Exception):
    pass


# ── Multi-target Jump (Multiply / Duplicate) ────────────────────────────────

class MultiJumpStrategy(Enum):
    """Strategy for dispatching a session to multiple targets."""
    BROADCAST = "broadcast"   # Fire-and-forget to all; collect results
    MIRROR = "mirror"         # All must succeed or the whole operation fails
    RACE = "race"             # First successful delivery wins; cancel the rest
    CASCADE = "cascade"       # Sequential: each target only after the previous succeeds


@dataclass
class TargetResult:
    """Outcome of a jump attempt to a single target."""
    device: Device
    success: bool
    elapsed: float = 0.0
    error: Optional[str] = None
    retries: int = 0


@dataclass
class MultiJumpResult:
    """Aggregate outcome of a multi-target jump."""
    strategy: MultiJumpStrategy
    session_id: str
    targets: list  # list[TargetResult]
    started: float = 0.0
    finished: float = 0.0

    @property
    def succeeded(self) -> list:
        return [t for t in self.targets if t.success]

    @property
    def failed(self) -> list:
        return [t for t in self.targets if not t.success]

    @property
    def total_elapsed(self) -> float:
        return self.finished - self.started if self.finished else 0.0

    @property
    def all_ok(self) -> bool:
        return all(t.success for t in self.targets)

    @property
    def any_ok(self) -> bool:
        return any(t.success for t in self.targets)

    def summary(self) -> str:
        ok = len(self.succeeded)
        fail = len(self.failed)
        return (
            f"[{self.strategy.value.upper()}] {ok}/{ok + fail} targets reached "
            f"in {self.total_elapsed:.2f}s (session {self.session_id})"
        )


def _jump_single(
    target: Device,
    session: JumpSession,
    auth_token: str = None,
    timeout: float = 30.0,
    max_retries: int = 0,
) -> TargetResult:
    """Jump to one target with optional retries. Returns a TargetResult."""
    t0 = time.time()
    last_err = None
    for attempt in range(1 + max_retries):
        try:
            ok = jump_to_device(target, session, auth_token=auth_token,
                                timeout=timeout)
            return TargetResult(
                device=target, success=ok,
                elapsed=time.time() - t0, retries=attempt,
            )
        except (JumpError, OSError, ConnectionError) as exc:
            last_err = exc
            if attempt < max_retries:
                time.sleep(min(2 ** attempt, 8))
    return TargetResult(
        device=target, success=False,
        elapsed=time.time() - t0,
        error=str(last_err), retries=max_retries,
    )


def jump_to_devices(
    targets: list,
    session: JumpSession,
    *,
    strategy: MultiJumpStrategy = MultiJumpStrategy.BROADCAST,
    auth_token: str = None,
    timeout: float = 30.0,
    max_retries: int = 0,
    max_workers: int = 0,
    on_progress: Callable[[TargetResult, int, int], None] = None,
) -> MultiJumpResult:
    """Jump a session to multiple targets using the chosen strategy.

    Args:
        targets: Devices to send the session to.
        session: The session to transfer.
        strategy: Dispatch strategy (BROADCAST, MIRROR, RACE, CASCADE).
        auth_token: Shared auth token for all targets.
        timeout: Per-target TCP timeout.
        max_retries: Per-target retry count (with exponential backoff).
        max_workers: Thread pool size (0 = len(targets)).
        on_progress: Callback(result, completed_count, total) after each target.

    Returns:
        MultiJumpResult with per-target outcomes.
    """
    if not targets:
        return MultiJumpResult(
            strategy=strategy, session_id=session.session_id,
            targets=[], started=time.time(), finished=time.time(),
        )

    workers = max_workers or min(len(targets), 16)
    result = MultiJumpResult(
        strategy=strategy, session_id=session.session_id,
        targets=[], started=time.time(),
    )

    if strategy == MultiJumpStrategy.CASCADE:
        return _cascade_jump(targets, session, result,
                             auth_token, timeout, max_retries, on_progress)

    if strategy == MultiJumpStrategy.RACE:
        return _race_jump(targets, session, result, workers,
                          auth_token, timeout, max_retries, on_progress)

    # BROADCAST and MIRROR: dispatch all concurrently
    completed = 0
    cancel = threading.Event()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_jump_single, t, session, auth_token, timeout,
                        max_retries): t
            for t in targets
        }
        for future in as_completed(futures):
            tr = future.result()
            result.targets.append(tr)
            completed += 1
            if on_progress:
                on_progress(tr, completed, len(targets))
            # MIRROR: abort early on first failure
            if strategy == MultiJumpStrategy.MIRROR and not tr.success:
                cancel.set()
                for f in futures:
                    f.cancel()
                break

    result.finished = time.time()
    return result


def _cascade_jump(targets, session, result, auth_token, timeout,
                  max_retries, on_progress):
    """Sequential jump — each target only attempted after the previous succeeds."""
    for i, target in enumerate(targets):
        tr = _jump_single(target, session, auth_token, timeout, max_retries)
        result.targets.append(tr)
        if on_progress:
            on_progress(tr, i + 1, len(targets))
        if not tr.success:
            break
    result.finished = time.time()
    return result


def _race_jump(targets, session, result, workers, auth_token, timeout,
               max_retries, on_progress):
    """First successful delivery wins; remaining futures are cancelled."""
    winner_found = threading.Event()

    def _race_single(target):
        if winner_found.is_set():
            return TargetResult(device=target, success=False,
                                error="cancelled (race lost)")
        tr = _jump_single(target, session, auth_token, timeout, max_retries)
        if tr.success:
            winner_found.set()
        return tr

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_race_single, t): t for t in targets}
        for future in as_completed(futures):
            tr = future.result()
            result.targets.append(tr)
            completed += 1
            if on_progress:
                on_progress(tr, completed, len(targets))

    result.finished = time.time()
    return result


class JumpNode:
    """A node that can both send and receive jump sessions."""

    def __init__(self, node_name: str = None, listen_port: int = 47701,
                 auth_token: str = None,
                 on_session_received=None):
        self.node_name = node_name or socket.gethostname()
        self.listen_port = listen_port
        self.auth_token = auth_token
        self.on_session_received = on_session_received
        self.discovery = DiscoveryManager(
            node_name=self.node_name,
            listen_port=listen_port,
        )
        self.listener = JumpListener(
            port=listen_port,
            auth_validator=self._validate_auth if auth_token else None,
            on_connection=self._handle_connection,
        )
        self.received_sessions: list[JumpSession] = []

    def _validate_auth(self, token: str) -> bool:
        return token == self.auth_token

    def _handle_connection(self, conn: JumpConnection):
        try:
            session = receive_session(conn)
            self.received_sessions.append(session)
            if self.on_session_received:
                self.on_session_received(session)
        except (ProtocolError, ValueError, ConnectionError):
            pass
        finally:
            conn.close()

    def start(self):
        self.discovery.start()
        self.listener.start()

    def stop(self):
        self.discovery.stop()
        self.listener.stop()

    def discover_targets(self) -> list[Device]:
        return self.discovery.get_all_devices()

    def jump(self, target: Device, session_id: str = None,
             include_env: bool = True, include_files: list[str] = None,
             extra_metadata: dict = None) -> bool:
        sid = session_id or f"jump-{int(time.time())}"
        session = capture_session(
            session_id=sid,
            source_device=self.discovery.node_id,
            include_env=include_env,
            include_files=include_files,
            extra_metadata=extra_metadata,
        )
        return jump_to_device(target, session, auth_token=self.auth_token)

    def multi_jump(
        self,
        targets: list = None,
        *,
        strategy: MultiJumpStrategy = MultiJumpStrategy.BROADCAST,
        session_id: str = None,
        include_env: bool = True,
        include_files: list[str] = None,
        extra_metadata: dict = None,
        max_retries: int = 0,
        max_workers: int = 0,
        on_progress: Callable[[TargetResult, int, int], None] = None,
    ) -> MultiJumpResult:
        """Multiply / duplicate this session to multiple targets.

        Args:
            targets: Devices to jump to. If None, discovers all available.
            strategy: BROADCAST, MIRROR, RACE, or CASCADE.
            session_id: Custom session ID.
            include_env: Include environment variables.
            include_files: Files to attach.
            extra_metadata: Arbitrary metadata dict.
            max_retries: Per-target retries with exponential backoff.
            max_workers: Thread pool size (0 = auto).
            on_progress: Callback after each target completes.

        Returns:
            MultiJumpResult with per-target outcomes.
        """
        if targets is None:
            targets = self.discover_targets()

        if not targets:
            logger.warning("multi_jump: no targets found")
            return MultiJumpResult(
                strategy=strategy,
                session_id=session_id or "empty",
                targets=[],
                started=time.time(),
                finished=time.time(),
            )

        sid = session_id or f"multi-{int(time.time())}"
        session = capture_session(
            session_id=sid,
            source_device=self.discovery.node_id,
            include_env=include_env,
            include_files=include_files,
            extra_metadata={
                **(extra_metadata or {}),
                "multi_jump": True,
                "strategy": strategy.value,
                "target_count": len(targets),
            },
        )

        return jump_to_devices(
            targets, session,
            strategy=strategy,
            auth_token=self.auth_token,
            max_retries=max_retries,
            max_workers=max_workers,
            on_progress=on_progress,
        )
