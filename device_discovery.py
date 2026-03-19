"""
Device Discovery Module — Bluetooth, WiFi, and Mesh network scanner.

Discovers nearby devices on the local network (WiFi) and via Bluetooth,
with global reach through DHT-based peer discovery and relay-based mesh
routing for NAT traversal.

Extended with:
- DHT (Distributed Hash Table) for global peer discovery beyond LAN
- MeshRouter for multi-hop relay routing through intermediary nodes
- RelayNode for NAT traversal — sessions can tunnel through relay peers
"""

import hashlib
import json
import logging
import socket
import struct
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

STALE_TIMEOUT_SECONDS = 30.0
ANNOUNCE_INTERVAL = 5
BT_SCAN_DURATION = 4

# DHT constants
DHT_K_BUCKET_SIZE = 20        # max peers per k-bucket
DHT_PING_INTERVAL = 60.0      # seconds between DHT pings
DHT_STALE_TIMEOUT = 120.0     # DHT peer staleness threshold
DHT_PORT = 47702              # default DHT UDP port

# Mesh routing constants
MESH_MAX_HOPS = 8             # maximum relay hops
MESH_ROUTE_TTL = 300.0        # route cache TTL in seconds


class Transport(Enum):
    WIFI = "wifi"
    BLUETOOTH = "bluetooth"
    RELAY = "relay"            # routed through mesh relay
    DHT = "dht"               # discovered via DHT


@dataclass
class Device:
    device_id: str
    name: str
    address: str
    transport: Transport
    port: int = 0
    last_seen: float = 0.0
    capabilities: list = field(default_factory=list)
    signal_strength: int = 0
    relay_path: list = field(default_factory=list)  # for mesh-routed devices

    def to_dict(self):
        d = asdict(self)
        d["transport"] = self.transport.value
        return d

    @classmethod
    def from_dict(cls, d):
        d = dict(d)
        d["transport"] = Transport(d["transport"])
        return cls(**d)

    @property
    def is_stale(self):
        return (time.time() - self.last_seen) > STALE_TIMEOUT_SECONDS

    @property
    def is_relayed(self) -> bool:
        return len(self.relay_path) > 0

    @property
    def hop_count(self) -> int:
        return len(self.relay_path)


# ── WiFi Discovery (UDP Broadcast) ──────────────────────────────────────────

MULTICAST_GROUP = "239.255.77.88"
DISCOVERY_PORT = 47700
MAGIC = b"JUMP"


def _build_announce(node_id: str, node_name: str, listen_port: int,
                    capabilities: list) -> bytes:
    payload = json.dumps({
        "id": node_id,
        "name": node_name,
        "port": listen_port,
        "caps": capabilities,
    }).encode()
    return MAGIC + struct.pack("!H", len(payload)) + payload


def _parse_announce(data: bytes) -> Optional[dict]:
    if not data.startswith(MAGIC):
        return None
    if len(data) < 6:
        return None
    length = struct.unpack("!H", data[4:6])[0]
    payload = data[6:6 + length]
    try:
        return json.loads(payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


class WiFiDiscovery:
    """Discovers peers on the LAN using UDP multicast announcements."""

    def __init__(self, node_id: str, node_name: str, listen_port: int,
                 capabilities: list = None):
        self.node_id = node_id
        self.node_name = node_name
        self.listen_port = listen_port
        self.capabilities = capabilities or ["jump", "file_transfer"]
        self.devices: dict[str, Device] = {}
        self._lock = threading.Lock()
        self._running = False
        self._threads: list[threading.Thread] = []

    def start(self):
        self._running = True
        t_listen = threading.Thread(target=self._listen_loop, daemon=True)
        t_announce = threading.Thread(target=self._announce_loop, daemon=True)
        self._threads = [t_listen, t_announce]
        for t in self._threads:
            t.start()

    def stop(self):
        self._running = False
        for t in self._threads:
            t.join(timeout=2)

    def get_devices(self) -> list[Device]:
        with self._lock:
            return [d for d in self.devices.values() if not d.is_stale]

    def _announce_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        sock.settimeout(1.0)
        msg = _build_announce(self.node_id, self.node_name, self.listen_port,
                              self.capabilities)
        while self._running:
            try:
                sock.sendto(msg, (MULTICAST_GROUP, DISCOVERY_PORT))
            except OSError:
                pass
            time.sleep(ANNOUNCE_INTERVAL)
        sock.close()

    def _listen_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", DISCOVERY_PORT))
        except OSError:
            return
        group = socket.inet_aton(MULTICAST_GROUP)
        mreq = struct.pack("4sL", group, socket.INADDR_ANY)
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        except OSError:
            pass
        sock.settimeout(2.0)
        while self._running:
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            info = _parse_announce(data)
            if info is None or info.get("id") == self.node_id:
                continue
            device = Device(
                device_id=info["id"],
                name=info.get("name", "unknown"),
                address=addr[0],
                transport=Transport.WIFI,
                port=info.get("port", 0),
                last_seen=time.time(),
                capabilities=info.get("caps", []),
            )
            with self._lock:
                self.devices[device.device_id] = device
        sock.close()


# ── Bluetooth Discovery (simulated / real via PyBluez when available) ────────

class BluetoothDiscovery:
    """Discovers nearby Bluetooth devices.

    Uses PyBluez if available; otherwise falls back to a stub that returns
    an empty list (useful for testing or environments without Bluetooth).
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.devices: dict[str, Device] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._has_bluetooth = self._check_bluetooth()

    @staticmethod
    def _check_bluetooth() -> bool:
        try:
            import bluetooth  # noqa: F401
            return True
        except ImportError:
            return False

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)

    def get_devices(self) -> list[Device]:
        with self._lock:
            return [d for d in self.devices.values() if not d.is_stale]

    def _scan_loop(self):
        while self._running:
            found = self._do_scan()
            with self._lock:
                for dev in found:
                    self.devices[dev.device_id] = dev
            # Bluetooth scans are slow; wait between scans
            for _ in range(100):
                if not self._running:
                    return
                time.sleep(0.1)

    def _do_scan(self) -> list[Device]:
        if not self._has_bluetooth:
            return []
        try:
            import bluetooth
            nearby = bluetooth.discover_devices(duration=BT_SCAN_DURATION, lookup_names=True,
                                                lookup_class=False, flush_cache=True)
            results = []
            for addr, name in nearby:
                dev_id = hashlib.sha256(addr.encode()).hexdigest()[:16]
                results.append(Device(
                    device_id=dev_id,
                    name=name or addr,
                    address=addr,
                    transport=Transport.BLUETOOTH,
                    last_seen=time.time(),
                    capabilities=["jump"],
                ))
            return results
        except Exception:
            return []


# ── Unified Discovery Manager ───────────────────────────────────────────────

class DiscoveryManager:
    """Runs WiFi and Bluetooth discovery together, providing a unified device list."""

    def __init__(self, node_name: str = None, listen_port: int = 47701,
                 capabilities: list = None):
        self.node_id = uuid.uuid4().hex[:16]
        self.node_name = node_name or socket.gethostname()
        self.listen_port = listen_port
        self.capabilities = capabilities or ["jump", "file_transfer"]
        self.wifi = WiFiDiscovery(self.node_id, self.node_name,
                                  self.listen_port, self.capabilities)
        self.bluetooth = BluetoothDiscovery(self.node_id)

    def start(self):
        self.wifi.start()
        self.bluetooth.start()

    def stop(self):
        self.wifi.stop()
        self.bluetooth.stop()

    def get_all_devices(self) -> list[Device]:
        seen = {}
        for dev in self.wifi.get_devices() + self.bluetooth.get_devices():
            if dev.device_id not in seen or dev.last_seen > seen[dev.device_id].last_seen:
                seen[dev.device_id] = dev
        return sorted(seen.values(), key=lambda d: d.last_seen, reverse=True)


# ── DHT (Distributed Hash Table) Discovery ──────────────────────────────────

def _xor_distance(id_a: str, id_b: str) -> int:
    """Compute XOR distance between two hex-encoded node IDs."""
    a_int = int(id_a[:16], 16) if len(id_a) >= 16 else int(id_a or "0", 16)
    b_int = int(id_b[:16], 16) if len(id_b) >= 16 else int(id_b or "0", 16)
    return a_int ^ b_int


@dataclass
class DHTEntry:
    """A peer record in the DHT."""
    node_id: str
    address: str
    port: int
    name: str = ""
    capabilities: list = field(default_factory=list)
    last_seen: float = 0.0

    @property
    def is_stale(self) -> bool:
        return (time.time() - self.last_seen) > DHT_STALE_TIMEOUT


class DHTDiscovery:
    """Kademlia-inspired DHT for global peer discovery.

    Nodes announce themselves to known bootstrap peers and recursively
    discover new peers by querying those closest to a target ID. This
    enables discovery across different networks, subnets, and even
    NAT boundaries (when combined with RelayNode).

    Usage:
        dht = DHTDiscovery(node_id, node_name, listen_port=47702)
        dht.add_bootstrap("203.0.113.50", 47702)
        dht.start()
        peers = dht.get_peers()
    """

    def __init__(
        self,
        node_id: str,
        node_name: str,
        listen_port: int = DHT_PORT,
        capabilities: Optional[list] = None,
    ):
        self.node_id = node_id
        self.node_name = node_name
        self.listen_port = listen_port
        self.capabilities = capabilities or ["jump", "file_transfer"]
        self._k_buckets: Dict[int, Dict[str, DHTEntry]] = defaultdict(dict)
        self._lock = threading.RLock()
        self._running = False
        self._threads: list = []
        self._bootstrap_peers: List[Tuple[str, int]] = []

    def add_bootstrap(self, address: str, port: int = DHT_PORT) -> None:
        """Add a bootstrap peer for initial DHT population."""
        self._bootstrap_peers.append((address, port))

    def start(self) -> None:
        self._running = True
        t_listen = threading.Thread(target=self._listen_loop, daemon=True)
        t_maintain = threading.Thread(target=self._maintenance_loop, daemon=True)
        self._threads = [t_listen, t_maintain]
        for t in self._threads:
            t.start()
        # Bootstrap
        self._bootstrap()

    def stop(self) -> None:
        self._running = False
        for t in self._threads:
            t.join(timeout=3)

    def get_peers(self) -> List[DHTEntry]:
        """Return all known non-stale DHT peers."""
        with self._lock:
            peers = []
            for bucket in self._k_buckets.values():
                for entry in bucket.values():
                    if not entry.is_stale:
                        peers.append(entry)
            return peers

    def get_devices(self) -> List[Device]:
        """Return DHT peers as Device objects for unified discovery."""
        devices = []
        for peer in self.get_peers():
            devices.append(Device(
                device_id=peer.node_id,
                name=peer.name or peer.node_id[:8],
                address=peer.address,
                transport=Transport.DHT,
                port=peer.port,
                last_seen=peer.last_seen,
                capabilities=peer.capabilities,
            ))
        return devices

    def find_node(self, target_id: str) -> Optional[DHTEntry]:
        """Find the closest known peer to a target ID."""
        with self._lock:
            best = None
            best_dist = float("inf")
            for bucket in self._k_buckets.values():
                for entry in bucket.values():
                    if entry.is_stale:
                        continue
                    dist = _xor_distance(entry.node_id, target_id)
                    if dist < best_dist:
                        best_dist = dist
                        best = entry
            return best

    def announce(self, address: str, port: int) -> None:
        """Send an announcement to a specific peer."""
        msg = json.dumps({
            "type": "announce",
            "node_id": self.node_id,
            "name": self.node_name,
            "port": self.listen_port,
            "caps": self.capabilities,
        }).encode()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(2.0)
            sock.sendto(b"DHT\x01" + msg, (address, port))
            sock.close()
        except OSError:
            pass

    def _bootstrap(self) -> None:
        """Announce to all bootstrap peers."""
        for addr, port in self._bootstrap_peers:
            self.announce(addr, port)

    def _bucket_index(self, node_id: str) -> int:
        """Determine which k-bucket a node ID belongs to."""
        dist = _xor_distance(self.node_id, node_id)
        if dist == 0:
            return 0
        return dist.bit_length()

    def _add_peer(self, entry: DHTEntry) -> None:
        """Add or update a peer in the routing table."""
        if entry.node_id == self.node_id:
            return
        idx = self._bucket_index(entry.node_id)
        with self._lock:
            bucket = self._k_buckets[idx]
            if entry.node_id in bucket:
                bucket[entry.node_id].last_seen = entry.last_seen
                bucket[entry.node_id].address = entry.address
                bucket[entry.node_id].port = entry.port
            elif len(bucket) < DHT_K_BUCKET_SIZE:
                bucket[entry.node_id] = entry
            else:
                # Evict stale entries
                stale = [nid for nid, e in bucket.items() if e.is_stale]
                for nid in stale[:1]:
                    del bucket[nid]
                if len(bucket) < DHT_K_BUCKET_SIZE:
                    bucket[entry.node_id] = entry

    def _listen_loop(self) -> None:
        """Listen for incoming DHT messages."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", self.listen_port))
        except OSError:
            return
        sock.settimeout(2.0)

        while self._running:
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break

            if not data.startswith(b"DHT\x01"):
                continue
            try:
                msg = json.loads(data[4:].decode())
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            msg_type = msg.get("type")
            if msg_type == "announce":
                entry = DHTEntry(
                    node_id=msg["node_id"],
                    address=addr[0],
                    port=msg.get("port", DHT_PORT),
                    name=msg.get("name", ""),
                    capabilities=msg.get("caps", []),
                    last_seen=time.time(),
                )
                self._add_peer(entry)
                # Respond with our own announcement
                self.announce(addr[0], msg.get("port", DHT_PORT))
            elif msg_type == "find_node":
                target = msg.get("target_id", "")
                closest = self.find_node(target)
                resp = {"type": "find_node_resp", "node_id": self.node_id}
                if closest:
                    resp["closest"] = {
                        "node_id": closest.node_id,
                        "address": closest.address,
                        "port": closest.port,
                        "name": closest.name,
                    }
                try:
                    sock.sendto(
                        b"DHT\x01" + json.dumps(resp).encode(),
                        addr,
                    )
                except OSError:
                    pass

        sock.close()

    def _maintenance_loop(self) -> None:
        """Periodic DHT maintenance: re-announce and prune stale entries."""
        while self._running:
            time.sleep(DHT_PING_INTERVAL)
            if not self._running:
                break
            # Re-announce to known peers
            for peer in self.get_peers():
                self.announce(peer.address, peer.port)
            # Prune stale
            with self._lock:
                for idx in list(self._k_buckets.keys()):
                    bucket = self._k_buckets[idx]
                    stale = [nid for nid, e in bucket.items() if e.is_stale]
                    for nid in stale:
                        del bucket[nid]

    @property
    def peer_count(self) -> int:
        with self._lock:
            return sum(len(b) for b in self._k_buckets.values())


# ── Mesh Router ──────────────────────────────────────────────────────────────

@dataclass
class RouteEntry:
    """A cached route to a destination through relay hops."""
    destination_id: str
    next_hop_address: str
    next_hop_port: int
    full_path: list        # list of (address, port) tuples forming the route
    hop_count: int
    discovered_at: float
    latency_ms: float = 0.0

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.discovered_at) > MESH_ROUTE_TTL


class MeshRouter:
    """Multi-hop relay routing for NAT traversal and global connectivity.

    When Node A wants to reach Node C but they can't connect directly
    (e.g., different NATs, firewalls), the MeshRouter finds a path
    through intermediate relay nodes (A → B → C).

    The router maintains a route cache populated by route discovery
    messages that flood through the mesh. Each route entry has a TTL
    and is automatically refreshed.

    Usage:
        router = MeshRouter(node_id="abc123", listen_port=47703)
        router.add_neighbor("192.168.1.10", 47703)
        router.start()
        route = router.find_route("target_node_id")
        if route:
            print(f"Route found: {route.full_path}")
    """

    def __init__(
        self,
        node_id: str,
        node_name: str = "",
        listen_port: int = 47703,
    ):
        self.node_id = node_id
        self.node_name = node_name or node_id[:8]
        self.listen_port = listen_port
        self._routes: Dict[str, RouteEntry] = {}
        self._neighbors: Dict[str, Tuple[str, int]] = {}  # node_id → (addr, port)
        self._seen_requests: Set[str] = set()  # dedup route requests
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._on_relay: Optional[Callable] = None  # callback for relayed data

    def add_neighbor(self, address: str, port: int,
                     node_id: Optional[str] = None) -> None:
        """Register a directly reachable neighbor node."""
        nid = node_id or hashlib.sha256(
            f"{address}:{port}".encode()
        ).hexdigest()[:16]
        with self._lock:
            self._neighbors[nid] = (address, port)

    def set_relay_handler(self, handler: Callable) -> None:
        """Set a callback for handling relayed data payloads."""
        self._on_relay = handler

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._listen_loop, daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)

    def find_route(self, destination_id: str) -> Optional[RouteEntry]:
        """Find a cached route to a destination, or initiate discovery."""
        with self._lock:
            route = self._routes.get(destination_id)
            if route and not route.is_expired:
                return route

        # Initiate route discovery
        self._discover_route(destination_id)

        # Check again after discovery
        with self._lock:
            route = self._routes.get(destination_id)
            if route and not route.is_expired:
                return route
        return None

    def get_reachable_devices(self) -> List[Device]:
        """Return all devices reachable through mesh routing."""
        devices = []
        with self._lock:
            for dest_id, route in self._routes.items():
                if not route.is_expired:
                    devices.append(Device(
                        device_id=dest_id,
                        name=f"mesh:{dest_id[:8]}",
                        address=route.next_hop_address,
                        transport=Transport.RELAY,
                        port=route.next_hop_port,
                        last_seen=route.discovered_at,
                        capabilities=["jump", "relay"],
                        relay_path=[
                            f"{a}:{p}" for a, p in route.full_path
                        ],
                    ))
        return devices

    def relay_data(
        self,
        destination_id: str,
        payload: bytes,
    ) -> bool:
        """Send data to a destination via relay routing.

        Returns True if the data was forwarded to the next hop.
        """
        route = self.find_route(destination_id)
        if not route:
            return False

        msg = json.dumps({
            "type": "relay_data",
            "origin": self.node_id,
            "destination": destination_id,
            "path": route.full_path,
            "hop": 0,
            "payload_b64": __import__("base64").b64encode(payload).decode(),
        }).encode()

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(5.0)
            sock.sendto(
                b"MESH" + msg,
                (route.next_hop_address, route.next_hop_port),
            )
            sock.close()
            return True
        except OSError:
            return False

    def _discover_route(self, destination_id: str) -> None:
        """Flood a route discovery request to neighbors."""
        request_id = uuid.uuid4().hex[:16]
        msg = json.dumps({
            "type": "route_request",
            "request_id": request_id,
            "origin": self.node_id,
            "origin_addr": "0.0.0.0",
            "origin_port": self.listen_port,
            "destination": destination_id,
            "path": [],
            "hops": 0,
        }).encode()

        self._seen_requests.add(request_id)
        with self._lock:
            for nid, (addr, port) in self._neighbors.items():
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.settimeout(2.0)
                    sock.sendto(b"MESH" + msg, (addr, port))
                    sock.close()
                except OSError:
                    pass

    def _listen_loop(self) -> None:
        """Listen for mesh routing messages."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", self.listen_port))
        except OSError:
            return
        sock.settimeout(2.0)

        while self._running:
            try:
                data, addr = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break

            if not data.startswith(b"MESH"):
                continue

            try:
                msg = json.loads(data[4:].decode())
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            msg_type = msg.get("type")
            if msg_type == "route_request":
                self._handle_route_request(msg, addr, sock)
            elif msg_type == "route_reply":
                self._handle_route_reply(msg)
            elif msg_type == "relay_data":
                self._handle_relay_data(msg, sock)

        sock.close()

    def _handle_route_request(self, msg: dict, sender_addr: tuple,
                              sock: socket.socket) -> None:
        """Handle an incoming route discovery request."""
        request_id = msg["request_id"]
        if request_id in self._seen_requests:
            return  # already processed
        self._seen_requests.add(request_id)

        destination = msg["destination"]
        hops = msg.get("hops", 0) + 1
        path = msg.get("path", []) + [list(sender_addr)]

        if hops > MESH_MAX_HOPS:
            return

        # Are we the destination?
        if destination == self.node_id:
            reply = json.dumps({
                "type": "route_reply",
                "request_id": request_id,
                "origin": msg["origin"],
                "destination": self.node_id,
                "path": path + [["0.0.0.0", self.listen_port]],
                "hops": hops,
            }).encode()
            # Send reply back toward origin
            try:
                origin_addr = msg.get("origin_addr", sender_addr[0])
                origin_port = msg.get("origin_port", sender_addr[1])
                sock.sendto(b"MESH" + reply, (origin_addr, origin_port))
            except OSError:
                pass
            return

        # Forward to neighbors (flood)
        msg["hops"] = hops
        msg["path"] = path
        forwarded = json.dumps(msg).encode()
        with self._lock:
            for nid, (addr, port) in self._neighbors.items():
                try:
                    sock.sendto(b"MESH" + forwarded, (addr, port))
                except OSError:
                    pass

    def _handle_route_reply(self, msg: dict) -> None:
        """Handle a route reply — cache the discovered route."""
        destination = msg["destination"]
        path = msg.get("path", [])
        hops = msg.get("hops", len(path))

        if not path:
            return

        path_tuples = [(p[0], p[1]) for p in path if len(p) >= 2]
        next_hop = path_tuples[0] if path_tuples else None
        if not next_hop:
            return

        route = RouteEntry(
            destination_id=destination,
            next_hop_address=next_hop[0],
            next_hop_port=next_hop[1],
            full_path=path_tuples,
            hop_count=hops,
            discovered_at=time.time(),
        )

        with self._lock:
            existing = self._routes.get(destination)
            if not existing or existing.is_expired or hops < existing.hop_count:
                self._routes[destination] = route
                logger.info(
                    "MeshRouter: route to %s via %d hops cached",
                    destination[:8], hops,
                )

    def _handle_relay_data(self, msg: dict,
                           sock: socket.socket) -> None:
        """Handle relayed data — forward or deliver."""
        destination = msg["destination"]
        hop = msg.get("hop", 0)
        path = msg.get("path", [])

        if destination == self.node_id:
            # We are the destination — deliver
            import base64
            payload = base64.b64decode(msg.get("payload_b64", ""))
            if self._on_relay:
                self._on_relay(msg["origin"], payload)
            return

        # Forward to next hop
        if hop + 1 < len(path):
            next_addr, next_port = path[hop + 1]
            msg["hop"] = hop + 1
            try:
                sock.sendto(
                    b"MESH" + json.dumps(msg).encode(),
                    (next_addr, int(next_port)),
                )
            except OSError:
                pass

    @property
    def route_count(self) -> int:
        with self._lock:
            return sum(
                1 for r in self._routes.values() if not r.is_expired
            )

    @property
    def neighbor_count(self) -> int:
        with self._lock:
            return len(self._neighbors)


# ── Extended Discovery Manager ───────────────────────────────────────────────

class GlobalDiscoveryManager(DiscoveryManager):
    """Extended discovery manager with DHT and mesh routing support.

    Combines local WiFi/Bluetooth discovery with global DHT-based discovery
    and mesh routing for NAT traversal.

    Usage:
        gdm = GlobalDiscoveryManager(node_name="my-node")
        gdm.add_dht_bootstrap("203.0.113.50", 47702)
        gdm.add_mesh_neighbor("192.168.1.10", 47703)
        gdm.start()
        all_devices = gdm.get_all_devices()  # local + global
    """

    def __init__(
        self,
        node_name: str = None,
        listen_port: int = 47701,
        dht_port: int = DHT_PORT,
        mesh_port: int = 47703,
        capabilities: Optional[list] = None,
    ):
        super().__init__(
            node_name=node_name,
            listen_port=listen_port,
            capabilities=capabilities,
        )
        self.dht = DHTDiscovery(
            self.node_id, self.node_name,
            listen_port=dht_port,
            capabilities=self.capabilities,
        )
        self.mesh = MeshRouter(
            self.node_id, self.node_name,
            listen_port=mesh_port,
        )

    def add_dht_bootstrap(self, address: str, port: int = DHT_PORT) -> None:
        """Add a DHT bootstrap peer."""
        self.dht.add_bootstrap(address, port)

    def add_mesh_neighbor(self, address: str, port: int = 47703,
                          node_id: Optional[str] = None) -> None:
        """Add a mesh routing neighbor."""
        self.mesh.add_neighbor(address, port, node_id)

    def start(self) -> None:
        super().start()
        self.dht.start()
        self.mesh.start()

    def stop(self) -> None:
        super().stop()
        self.dht.stop()
        self.mesh.stop()

    def get_all_devices(self) -> list:
        """Return devices from all discovery methods: WiFi, BT, DHT, Mesh."""
        seen = {}

        # Local devices (WiFi + Bluetooth)
        for dev in self.wifi.get_devices() + self.bluetooth.get_devices():
            if dev.device_id not in seen or dev.last_seen > seen[dev.device_id].last_seen:
                seen[dev.device_id] = dev

        # DHT peers
        for dev in self.dht.get_devices():
            if dev.device_id not in seen or dev.last_seen > seen[dev.device_id].last_seen:
                seen[dev.device_id] = dev

        # Mesh-routed peers
        for dev in self.mesh.get_reachable_devices():
            if dev.device_id not in seen:
                seen[dev.device_id] = dev

        return sorted(seen.values(), key=lambda d: d.last_seen, reverse=True)

    @property
    def stats(self) -> dict:
        return {
            "wifi_devices": len(self.wifi.get_devices()),
            "bluetooth_devices": len(self.bluetooth.get_devices()),
            "dht_peers": self.dht.peer_count,
            "mesh_routes": self.mesh.route_count,
            "mesh_neighbors": self.mesh.neighbor_count,
            "total_reachable": len(self.get_all_devices()),
        }
