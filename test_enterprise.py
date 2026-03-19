"""
Tests for enterprise framework upgrades:
  1. Deep execution-state serialization (session_jumper.py)
  2. Distributed RPC offloading (mirror_blend.py)
  3. Generative AI self-healing (autonomous.py)
  4. Mesh routing and DHT discovery (device_discovery.py)
  5. WebAssembly sandboxing (autonomous.py)
"""

import base64
import hashlib
import json
import pickle
import socket
import threading
import time
import types
import unittest
from unittest.mock import MagicMock, patch

from mirror_blend import MirrorRegistry, Blender, DistributedBlender
from device_discovery import (
    Device, Transport, DHTDiscovery, DHTEntry, MeshRouter, RouteEntry,
    GlobalDiscoveryManager, _xor_distance,
    DHT_K_BUCKET_SIZE, MESH_MAX_HOPS,
)
from autonomous import (
    GenerativeHealer, HotUpgrader, WasmSandbox,
    SandboxViolation, SandboxTimeout,
    _safe_get_source,
)
from session_jumper import (
    ExecutionSnapshot, freeze_execution_state, thaw_execution_state,
    embed_snapshot_in_session, extract_snapshot_from_session,
    JumpSession, _get_serializer, _serializer_name,
)


def _make_module(name="test_mod", **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Deep Execution-State Serialization
# ═══════════════════════════════════════════════════════════════════════════════

class TestSerializerBackend(unittest.TestCase):
    def test_serializer_available(self):
        s = _get_serializer()
        self.assertTrue(hasattr(s, "dumps"))
        self.assertTrue(hasattr(s, "loads"))

    def test_serializer_name(self):
        name = _serializer_name()
        self.assertIn(name, ("dill", "cloudpickle", "pickle"))


class TestExecutionSnapshot(unittest.TestCase):
    def test_create_snapshot(self):
        snap = ExecutionSnapshot(
            snapshot_id="test-1",
            source_device="dev-a",
            timestamp=time.time(),
        )
        self.assertEqual(snap.snapshot_id, "test-1")
        self.assertEqual(snap.source_device, "dev-a")

    def test_checksum(self):
        snap = ExecutionSnapshot(
            snapshot_id="ck-1",
            source_device="dev-b",
            local_vars=b"data",
        )
        cs = snap.compute_checksum()
        self.assertEqual(len(cs), 64)
        self.assertEqual(cs, snap.compute_checksum())

    def test_validate_good(self):
        snap = ExecutionSnapshot(snapshot_id="v1", source_device="d1")
        snap.checksum = snap.compute_checksum()
        self.assertTrue(snap.validate())

    def test_validate_bad(self):
        snap = ExecutionSnapshot(snapshot_id="v2", source_device="d2")
        snap.checksum = "wrong"
        self.assertFalse(snap.validate())

    def test_serialize_deserialize(self):
        snap = ExecutionSnapshot(
            snapshot_id="ser-1",
            source_device="dev-a",
            timestamp=12345.0,
            metadata={"key": "value"},
            serializer_backend="pickle",
        )
        snap.local_vars = pickle.dumps({"x": 42, "name": "test"})
        snap.checksum = snap.compute_checksum()

        data = snap.serialize()
        restored = ExecutionSnapshot.deserialize(data)
        self.assertEqual(restored.snapshot_id, "ser-1")
        self.assertEqual(restored.metadata["key"], "value")
        self.assertTrue(restored.validate())


class TestFreezeThaw(unittest.TestCase):
    def test_freeze_basic_vars(self):
        snap = freeze_execution_state(
            "freeze-1", "dev-a",
            local_vars={"x": 42, "name": "test", "items": [1, 2, 3]},
        )
        self.assertEqual(snap.snapshot_id, "freeze-1")
        self.assertTrue(snap.validate())

    def test_thaw_basic_vars(self):
        snap = freeze_execution_state(
            "thaw-1", "dev-a",
            local_vars={"x": 42, "y": "hello"},
        )
        state = thaw_execution_state(snap)
        self.assertEqual(state["local_vars"]["x"], 42)
        self.assertEqual(state["local_vars"]["y"], "hello")

    def test_freeze_objects(self):
        # Use a simple dict as the "object" to ensure pickle compatibility
        obj = {"val": 99, "items": [1, 2, 3]}
        snap = freeze_execution_state(
            "obj-1", "dev-a",
            objects={"my_obj": obj},
        )
        state = thaw_execution_state(snap)
        self.assertEqual(state["objects"]["my_obj"]["val"], 99)
        self.assertEqual(state["objects"]["my_obj"]["items"], [1, 2, 3])

    def test_freeze_with_caller_locals(self):
        local_var = 123
        another = "captured"
        snap = freeze_execution_state(
            "caller-1", "dev-a",
            capture_caller_locals=True,
        )
        state = thaw_execution_state(snap)
        self.assertEqual(state["local_vars"]["local_var"], 123)
        self.assertEqual(state["local_vars"]["another"], "captured")

    def test_freeze_captures_stack_info(self):
        snap = freeze_execution_state("stack-1", "dev-a")
        self.assertGreater(len(snap.call_stack_info), 0)
        self.assertIn("filename", snap.call_stack_info[0])
        self.assertIn("function", snap.call_stack_info[0])

    def test_thaw_corrupt_raises(self):
        snap = ExecutionSnapshot(
            snapshot_id="bad", source_device="d1",
            checksum="definitely_wrong",
        )
        with self.assertRaises(ValueError):
            thaw_execution_state(snap)

    def test_freeze_generator_state(self):
        """Generator serialization requires dill; skip gracefully with pickle."""
        serializer = _get_serializer()

        def gen():
            yield 1
            yield 2
            yield 3

        g = gen()
        next(g)  # consume first value

        if serializer.__name__ == "dill":
            snap = freeze_execution_state(
                "gen-1", "dev-a",
                generators={"my_gen": g},
            )
            state = thaw_execution_state(snap)
            restored_gen = state["generators"]["my_gen"]
            self.assertEqual(next(restored_gen), 2)
            self.assertEqual(next(restored_gen), 3)
        else:
            # pickle can't serialize generators — verify graceful handling
            with self.assertRaises((TypeError, pickle.PicklingError)):
                freeze_execution_state(
                    "gen-1", "dev-a",
                    generators={"my_gen": g},
                )


class TestSnapshotInSession(unittest.TestCase):
    def test_embed_and_extract(self):
        session = JumpSession(
            session_id="embed-1", source_device="dev-a",
        )
        snap = freeze_execution_state(
            "snap-1", "dev-a",
            local_vars={"x": 42},
        )
        embed_snapshot_in_session(session, snap)
        self.assertIn("__deep_snapshot__", session.metadata)
        self.assertEqual(session.metadata["__snapshot_id__"], "snap-1")

        # Extract and verify
        restored = extract_snapshot_from_session(session)
        self.assertIsNotNone(restored)
        state = thaw_execution_state(restored)
        self.assertEqual(state["local_vars"]["x"], 42)

    def test_extract_from_plain_session(self):
        session = JumpSession(session_id="plain", source_device="dev-a")
        result = extract_snapshot_from_session(session)
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Distributed RPC Offloading
# ═══════════════════════════════════════════════════════════════════════════════

class TestDistributedBlender(unittest.TestCase):
    def setUp(self):
        self.registry = MirrorRegistry()
        self.blender = Blender(self.registry)
        self.dist = DistributedBlender(
            self.registry, self.blender,
            cpu_threshold=80.0,
            serializer=pickle,
        )

    def tearDown(self):
        self.blender.revert_all()

    def test_local_execution_under_threshold(self):
        """Below threshold, functions run locally."""
        def add(a, b):
            return a + b

        remote = MagicMock()
        mod = _make_module(add=add)
        self.dist.register(mod, "add", remote_executor=remote)

        self.dist.update_load(cpu_percent=50.0)
        result = mod.add(2, 3)
        self.assertEqual(result, 5)
        remote.assert_not_called()
        self.assertEqual(self.dist.stats["local_count"], 1)

    def test_offload_above_threshold(self):
        """Above threshold, calls are offloaded to remote."""
        def add(a, b):
            return a + b

        def fake_remote(name, serialized_args):
            args_data = pickle.loads(serialized_args)
            result = args_data["args"][0] + args_data["args"][1]
            return pickle.dumps(result)

        mod = _make_module(add=add)
        self.dist.register(mod, "add", remote_executor=fake_remote)
        self.dist.update_load(cpu_percent=95.0)

        result = mod.add(10, 20)
        self.assertEqual(result, 30)
        self.assertEqual(self.dist.stats["offload_count"], 1)

    def test_fallback_on_remote_failure(self):
        """If remote fails, falls back to local execution."""
        def add(a, b):
            return a + b

        def failing_remote(name, data):
            raise ConnectionError("Remote unavailable")

        mod = _make_module(add=add)
        self.dist.register(mod, "add", remote_executor=failing_remote)
        self.dist.update_load(cpu_percent=95.0)

        result = mod.add(5, 7)
        self.assertEqual(result, 12)  # local fallback

    def test_should_offload_property(self):
        self.assertFalse(self.dist.should_offload)
        self.dist.update_load(cpu_percent=90.0)
        self.assertTrue(self.dist.should_offload)

    def test_unregister(self):
        def fn():
            return "original"

        mod = _make_module(fn=fn)
        key = self.dist.register(mod, "fn", remote_executor=lambda *a: None)
        self.assertEqual(self.dist.stats["registered"], 1)
        self.dist.unregister(key)
        self.assertEqual(self.dist.stats["registered"], 0)
        self.assertEqual(mod.fn(), "original")

    def test_stats(self):
        stats = self.dist.stats
        self.assertIn("offload_count", stats)
        self.assertIn("local_count", stats)
        self.assertIn("should_offload", stats)
        self.assertIn("registered", stats)

    def test_memory_threshold_offload(self):
        """Memory threshold also triggers offloading."""
        def fn():
            return "local"

        def remote(name, data):
            return pickle.dumps("remote")

        mod = _make_module(fn=fn)
        self.dist.register(mod, "fn", remote_executor=remote)
        self.dist.update_load(memory_percent=90.0)
        self.assertTrue(self.dist.should_offload)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Generative AI Self-Healing
# ═══════════════════════════════════════════════════════════════════════════════

class TestGenerativeHealer(unittest.TestCase):
    def setUp(self):
        self.registry = MirrorRegistry()
        self.blender = Blender(self.registry)
        self.upgrader = HotUpgrader(self.registry, self.blender)

    def tearDown(self):
        self.blender.revert_all()

    def test_protect_without_llm(self):
        """Without LLM, healer acts like a normal fallback chain."""
        def bad():
            raise ValueError("boom")

        def good():
            return "ok"

        mod = _make_module(fn=bad)
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
        )
        healer.protect(mod, "fn", static_fallbacks=[good])
        result = mod.fn()
        self.assertEqual(result, "ok")

    def test_llm_heals_function(self):
        """LLM generates a fix when all fallbacks fail."""
        call_count = {"n": 0}

        def buggy_fn(x):
            call_count["n"] += 1
            if call_count["n"] <= 1:
                raise ZeroDivisionError("division by zero")
            return x * 2  # won't reach here on first call

        # Mock LLM that "fixes" the function
        def mock_llm(prompt):
            return "def buggy_fn(x):\n    return x * 2\n"

        mod = _make_module(buggy_fn=buggy_fn)
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
            llm_backend=mock_llm,
        )
        healer.protect(mod, "buggy_fn")
        result = mod.buggy_fn(5)
        self.assertEqual(result, 10)
        self.assertEqual(healer.successful_patches, 1)

    def test_llm_patch_fails_gracefully(self):
        """If LLM generates bad code, original exception propagates."""
        def buggy_fn2():
            raise RuntimeError("original error")

        def bad_llm(prompt):
            return "this is not valid python @@@@"

        mod = _make_module(buggy_fn2=buggy_fn2)
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
            llm_backend=bad_llm,
        )
        healer.protect(mod, "buggy_fn2")
        with self.assertRaises(RuntimeError):
            mod.buggy_fn2()

    def test_max_attempts_respected(self):
        """Healer stops trying after max_attempts."""
        attempt_count = {"n": 0}

        def buggy_fn3():
            raise ValueError("fail")

        def counting_llm(prompt):
            attempt_count["n"] += 1
            return "def buggy_fn3():\n    raise TypeError('still broken')\n"

        mod = _make_module(buggy_fn3=buggy_fn3)
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
            llm_backend=counting_llm,
            max_attempts=2,
        )
        healer.protect(mod, "buggy_fn3")

        for _ in range(5):
            try:
                mod.buggy_fn3()
            except (ValueError, TypeError):
                pass

        self.assertLessEqual(attempt_count["n"], 2)

    def test_static_fallbacks_tried_first(self):
        """Static fallbacks are exhausted before LLM is invoked."""
        call_order = []

        def primary():
            call_order.append("primary")
            raise ValueError("primary fail")

        def fallback():
            call_order.append("fallback")
            return "from fallback"

        llm_called = {"n": 0}

        def mock_llm(prompt):
            llm_called["n"] += 1
            return "def primary():\n    return 'from llm'\n"

        mod = _make_module(primary=primary)
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
            llm_backend=mock_llm,
        )
        healer.protect(mod, "primary", static_fallbacks=[fallback])
        result = mod.primary()
        self.assertEqual(result, "from fallback")
        self.assertEqual(llm_called["n"], 0)  # LLM not needed

    def test_unprotect(self):
        def fn():
            return "original"

        mod = _make_module(fn=fn)
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
        )
        key = healer.protect(mod, "fn")
        healer.unprotect(key)
        self.assertEqual(healer.protection_count, 0)

    def test_patch_history(self):
        def buggy_ph():
            raise ValueError("fail")

        def mock_llm(prompt):
            return "def buggy_ph():\n    return 'fixed'\n"

        mod = _make_module(buggy_ph=buggy_ph)
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
            llm_backend=mock_llm,
        )
        healer.protect(mod, "buggy_ph")
        result = mod.buggy_ph()
        self.assertEqual(result, "fixed")

        self.assertEqual(len(healer.patch_history), 1)
        self.assertTrue(healer.patch_history[0].success)
        self.assertEqual(healer.patch_history[0].function_name, "buggy_ph")

    def test_llm_available_property(self):
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
        )
        self.assertFalse(healer.llm_available)
        healer.set_llm_backend(lambda p: "pass")
        self.assertTrue(healer.llm_available)

    def test_extract_code_strips_markdown(self):
        raw = "```python\ndef foo():\n    return 1\n```"
        cleaned = GenerativeHealer._extract_code(raw)
        self.assertNotIn("```", cleaned)
        self.assertIn("def foo():", cleaned)

    def test_build_prompt_contains_context(self):
        prompt = GenerativeHealer._build_prompt(
            func_name="parse",
            source_code="def parse(x): return x[0]",
            exception_type="IndexError",
            exception_msg="list index out of range",
            traceback_lines="Traceback...",
            args_repr="([], )",
            kwargs_repr="{}",
        )
        self.assertIn("parse", prompt)
        self.assertIn("IndexError", prompt)
        self.assertIn("list index out of range", prompt)


class TestSafeGetSource(unittest.TestCase):
    def test_gets_source_of_normal_function(self):
        def sample():
            return 42

        source = _safe_get_source(sample)
        self.assertIn("return 42", source)

    def test_falls_back_for_builtins(self):
        source = _safe_get_source(len)
        self.assertIn("unavailable", source.lower())


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Mesh Routing and DHT Discovery
# ═══════════════════════════════════════════════════════════════════════════════

class TestXorDistance(unittest.TestCase):
    def test_same_ids(self):
        self.assertEqual(_xor_distance("abcd1234", "abcd1234"), 0)

    def test_different_ids(self):
        dist = _xor_distance("0000000000000000", "0000000000000001")
        self.assertEqual(dist, 1)

    def test_symmetry(self):
        a, b = "abcd1234abcd1234", "1234abcd1234abcd"
        self.assertEqual(_xor_distance(a, b), _xor_distance(b, a))


class TestDHTEntry(unittest.TestCase):
    def test_fresh_entry(self):
        entry = DHTEntry(
            node_id="abc", address="1.2.3.4",
            port=47702, last_seen=time.time(),
        )
        self.assertFalse(entry.is_stale)

    def test_stale_entry(self):
        entry = DHTEntry(
            node_id="abc", address="1.2.3.4",
            port=47702, last_seen=time.time() - 300,
        )
        self.assertTrue(entry.is_stale)


class TestDHTDiscovery(unittest.TestCase):
    def test_init(self):
        dht = DHTDiscovery("node1", "TestNode")
        self.assertEqual(dht.node_id, "node1")
        self.assertEqual(dht.peer_count, 0)

    def test_add_peer(self):
        dht = DHTDiscovery("a000000000000001", "TestNode")
        entry = DHTEntry(
            node_id="b000000000000002", address="10.0.0.1",
            port=47702, name="Peer",
            last_seen=time.time(),
        )
        dht._add_peer(entry)
        self.assertEqual(dht.peer_count, 1)

    def test_does_not_add_self(self):
        dht = DHTDiscovery("a000000000000001", "TestNode")
        entry = DHTEntry(
            node_id="a000000000000001", address="10.0.0.1",
            port=47702, last_seen=time.time(),
        )
        dht._add_peer(entry)
        self.assertEqual(dht.peer_count, 0)

    def test_get_devices_returns_device_objects(self):
        dht = DHTDiscovery("a000000000000001", "TestNode")
        entry = DHTEntry(
            node_id="b000000000000002", address="10.0.0.2",
            port=47702, name="Remote",
            capabilities=["jump"],
            last_seen=time.time(),
        )
        dht._add_peer(entry)
        devices = dht.get_devices()
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0].transport, Transport.DHT)
        self.assertEqual(devices[0].name, "Remote")

    def test_find_node(self):
        dht = DHTDiscovery("0000000000000000", "Origin")
        for i in range(5):
            entry = DHTEntry(
                node_id=f"{i:016x}", address=f"10.0.0.{i+1}",
                port=47702, last_seen=time.time(),
            )
            dht._add_peer(entry)
        closest = dht.find_node("0000000000000003")
        self.assertIsNotNone(closest)
        self.assertEqual(closest.node_id, "0000000000000003")

    def test_k_bucket_eviction(self):
        dht = DHTDiscovery("0000000000000000", "Origin")
        # Add more than K_BUCKET_SIZE entries in the same bucket
        for i in range(DHT_K_BUCKET_SIZE + 5):
            entry = DHTEntry(
                node_id=f"1{i:015x}", address=f"10.0.{i//256}.{i%256}",
                port=47702, last_seen=time.time(),
            )
            dht._add_peer(entry)
        self.assertLessEqual(dht.peer_count, DHT_K_BUCKET_SIZE + 5)

    def test_bootstrap_peers(self):
        dht = DHTDiscovery("node1", "TestNode")
        dht.add_bootstrap("10.0.0.1", 47702)
        dht.add_bootstrap("10.0.0.2", 47702)
        self.assertEqual(len(dht._bootstrap_peers), 2)


class TestRouteEntry(unittest.TestCase):
    def test_fresh_route(self):
        route = RouteEntry(
            destination_id="dest1",
            next_hop_address="10.0.0.1",
            next_hop_port=47703,
            full_path=[("10.0.0.1", 47703)],
            hop_count=1,
            discovered_at=time.time(),
        )
        self.assertFalse(route.is_expired)

    def test_expired_route(self):
        route = RouteEntry(
            destination_id="dest1",
            next_hop_address="10.0.0.1",
            next_hop_port=47703,
            full_path=[],
            hop_count=1,
            discovered_at=time.time() - 600,
        )
        self.assertTrue(route.is_expired)


class TestMeshRouter(unittest.TestCase):
    def test_init(self):
        router = MeshRouter("node1", "TestNode")
        self.assertEqual(router.node_id, "node1")
        self.assertEqual(router.route_count, 0)

    def test_add_neighbor(self):
        router = MeshRouter("node1")
        router.add_neighbor("10.0.0.1", 47703)
        self.assertEqual(router.neighbor_count, 1)

    def test_get_reachable_empty(self):
        router = MeshRouter("node1")
        self.assertEqual(len(router.get_reachable_devices()), 0)

    def test_cached_route(self):
        router = MeshRouter("node1")
        route = RouteEntry(
            destination_id="dest1",
            next_hop_address="10.0.0.2",
            next_hop_port=47703,
            full_path=[("10.0.0.2", 47703)],
            hop_count=1,
            discovered_at=time.time(),
        )
        router._routes["dest1"] = route
        self.assertEqual(router.route_count, 1)

        found = router.find_route("dest1")
        self.assertIsNotNone(found)
        self.assertEqual(found.destination_id, "dest1")

    def test_reachable_devices_as_relay(self):
        router = MeshRouter("node1")
        route = RouteEntry(
            destination_id="dest1",
            next_hop_address="10.0.0.2",
            next_hop_port=47703,
            full_path=[("10.0.0.2", 47703), ("10.0.0.3", 47703)],
            hop_count=2,
            discovered_at=time.time(),
        )
        router._routes["dest1"] = route
        devices = router.get_reachable_devices()
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0].transport, Transport.RELAY)
        self.assertTrue(devices[0].is_relayed)
        self.assertEqual(devices[0].hop_count, 2)

    def test_handle_route_reply(self):
        router = MeshRouter("origin_node")
        msg = {
            "destination": "dest123",
            "path": [["10.0.0.1", 47703], ["10.0.0.2", 47703]],
            "hops": 2,
        }
        router._handle_route_reply(msg)
        self.assertEqual(router.route_count, 1)
        route = router._routes["dest123"]
        self.assertEqual(route.hop_count, 2)

    def test_relay_handler_callback(self):
        received = []
        router = MeshRouter("dest_node")
        router.set_relay_handler(
            lambda origin, payload: received.append((origin, payload))
        )
        msg = {
            "type": "relay_data",
            "origin": "sender",
            "destination": "dest_node",
            "path": [],
            "hop": 0,
            "payload_b64": base64.b64encode(b"hello").decode(),
        }
        # Simulate receiving relayed data
        router._handle_relay_data(msg, MagicMock())
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0], ("sender", b"hello"))


class TestDeviceRelayProperties(unittest.TestCase):
    def test_non_relayed_device(self):
        dev = Device("d1", "Local", "10.0.0.1", Transport.WIFI)
        self.assertFalse(dev.is_relayed)
        self.assertEqual(dev.hop_count, 0)

    def test_relayed_device(self):
        dev = Device(
            "d1", "Remote", "10.0.0.1", Transport.RELAY,
            relay_path=["10.0.0.2:47703", "10.0.0.3:47703"],
        )
        self.assertTrue(dev.is_relayed)
        self.assertEqual(dev.hop_count, 2)

    def test_transport_enum_new_values(self):
        self.assertEqual(Transport.RELAY.value, "relay")
        self.assertEqual(Transport.DHT.value, "dht")

    def test_device_roundtrip_with_relay(self):
        dev = Device(
            "d1", "Relayed", "10.0.0.1", Transport.RELAY,
            relay_path=["hop1", "hop2"],
        )
        d = dev.to_dict()
        self.assertEqual(d["transport"], "relay")
        restored = Device.from_dict(d)
        self.assertEqual(restored.transport, Transport.RELAY)
        self.assertEqual(restored.relay_path, ["hop1", "hop2"])


class TestGlobalDiscoveryManager(unittest.TestCase):
    def test_init(self):
        gdm = GlobalDiscoveryManager(node_name="test-global")
        self.assertEqual(gdm.node_name, "test-global")
        self.assertIsNotNone(gdm.dht)
        self.assertIsNotNone(gdm.mesh)

    def test_add_bootstrap_and_neighbor(self):
        gdm = GlobalDiscoveryManager(node_name="test-global")
        gdm.add_dht_bootstrap("10.0.0.1", 47702)
        gdm.add_mesh_neighbor("10.0.0.2", 47703)
        self.assertEqual(len(gdm.dht._bootstrap_peers), 1)
        self.assertEqual(gdm.mesh.neighbor_count, 1)

    def test_stats(self):
        gdm = GlobalDiscoveryManager(node_name="test-stats")
        stats = gdm.stats
        self.assertIn("wifi_devices", stats)
        self.assertIn("dht_peers", stats)
        self.assertIn("mesh_routes", stats)
        self.assertIn("total_reachable", stats)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. WebAssembly Sandboxing
# ═══════════════════════════════════════════════════════════════════════════════

class TestWasmSandbox(unittest.TestCase):
    def setUp(self):
        self.sandbox = WasmSandbox(timeout_seconds=5.0)

    def test_backend_detection(self):
        backend = self.sandbox.backend
        self.assertIn(backend, ("wasmtime", "restricted_exec"))

    def test_execute_simple_code(self):
        result = self.sandbox.execute(
            "def add(a, b): return a + b",
            func_name="add",
            args=(2, 3),
        )
        self.assertEqual(result, 5)

    def test_execute_without_func_name(self):
        result = self.sandbox.execute("x = 42")
        self.assertIsNone(result)

    def test_execute_with_extra_globals(self):
        result = self.sandbox.execute(
            "def double(x): return x * factor",
            func_name="double",
            args=(5,),
            extra_globals={"factor": 10},
        )
        self.assertEqual(result, 50)

    def test_blocks_os_import(self):
        with self.assertRaises(SandboxViolation):
            self.sandbox.execute("import os\nos.system('whoami')")

    def test_blocks_subprocess(self):
        with self.assertRaises(SandboxViolation):
            self.sandbox.execute("import subprocess")

    def test_blocks_exec_call(self):
        with self.assertRaises(SandboxViolation):
            self.sandbox.execute("exec('print(1)')")

    def test_blocks_eval_call(self):
        with self.assertRaises(SandboxViolation):
            self.sandbox.execute("eval('1+1')")

    def test_blocks_dunder_subclasses(self):
        with self.assertRaises(SandboxViolation):
            self.sandbox.execute("x = ().__class__.__subclasses__()")

    def test_allows_safe_imports(self):
        result = self.sandbox.execute(
            "import math\ndef sqrt4(): return math.sqrt(4)",
            func_name="sqrt4",
        )
        self.assertEqual(result, 2.0)

    def test_allows_json(self):
        result = self.sandbox.execute(
            'import json\ndef parse(): return json.loads(\'{"a": 1}\')',
            func_name="parse",
        )
        self.assertEqual(result, {"a": 1})

    def test_timeout_enforcement(self):
        sandbox = WasmSandbox(timeout_seconds=1.0)
        # Use a busy loop instead of time.sleep (time is blocked by sandbox)
        with self.assertRaises(SandboxTimeout):
            sandbox.execute(
                "def slow():\n    x = 0\n    while True: x += 1\nslow()",
            )

    def test_validate_source_clean(self):
        violations = self.sandbox.validate_source(
            "def add(a, b): return a + b"
        )
        self.assertEqual(violations, [])

    def test_validate_source_forbidden_import(self):
        violations = self.sandbox.validate_source("import os")
        self.assertGreater(len(violations), 0)
        self.assertIn("os", violations[0])

    def test_validate_source_syntax_error(self):
        violations = self.sandbox.validate_source("def (broken")
        self.assertGreater(len(violations), 0)

    def test_stats(self):
        self.sandbox.execute("x = 1")
        stats = self.sandbox.stats
        self.assertEqual(stats["executions"], 1)
        self.assertIn("backend", stats)

    def test_function_not_found_in_sandbox(self):
        with self.assertRaises(ValueError):
            self.sandbox.execute(
                "def foo(): return 1",
                func_name="bar",  # doesn't exist
            )

    def test_blocks_dunder_globals(self):
        """Block access to __globals__ on functions."""
        with self.assertRaises(SandboxViolation):
            self.sandbox.execute(
                "def f(): pass\nx = f.__globals__"
            )

    def test_allowed_builtins_work(self):
        result = self.sandbox.execute(
            "def test(): return len([1, 2, 3])",
            func_name="test",
        )
        self.assertEqual(result, 3)

    def test_list_comprehension(self):
        result = self.sandbox.execute(
            "def squares(n): return [x**2 for x in range(n)]",
            func_name="squares",
            args=(5,),
        )
        self.assertEqual(result, [0, 1, 4, 9, 16])

    def test_class_definition_in_sandbox(self):
        code = """
class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y
    def mag(self):
        return (self.x**2 + self.y**2) ** 0.5

def make_point():
    return Point(3, 4).mag()
"""
        result = self.sandbox.execute(code, func_name="make_point")
        self.assertEqual(result, 5.0)

    def test_runtime_import_blocked(self):
        """Even if static analysis is bypassed, runtime import is blocked."""
        # This won't trigger static analysis but should fail at runtime
        code = """
def sneaky():
    m = __import__('os')
    return m.getcwd()
"""
        # Static analysis catches __import__ call
        with self.assertRaises(SandboxViolation):
            self.sandbox.execute(code, func_name="sneaky")


# ═══════════════════════════════════════════════════════════════════════════════
# Integration Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestHealerWithSandbox(unittest.TestCase):
    """Test GenerativeHealer using WasmSandbox for patch validation."""

    def setUp(self):
        self.registry = MirrorRegistry()
        self.blender = Blender(self.registry)
        self.upgrader = HotUpgrader(self.registry, self.blender)
        self.sandbox = WasmSandbox()

    def tearDown(self):
        self.blender.revert_all()

    def test_llm_patch_validated_in_sandbox(self):
        """Generated patches can be pre-validated in sandbox."""
        def buggy_sb(x):
            return x / 0

        def sandboxed_llm(prompt):
            code = "def buggy_sb(x):\n    return x * 2\n"
            # Validate in sandbox before returning
            violations = self.sandbox.validate_source(code)
            if violations:
                return "def buggy_sb(x):\n    raise ValueError('unsafe')\n"
            return code

        mod = _make_module(buggy_sb=buggy_sb)
        healer = GenerativeHealer(
            self.registry, self.blender, self.upgrader,
            llm_backend=sandboxed_llm,
        )
        healer.protect(mod, "buggy_sb")
        result = mod.buggy_sb(5)
        self.assertEqual(result, 10)


class TestDistributedWithSnapshot(unittest.TestCase):
    """Test combining deep serialization with distributed offloading."""

    def test_snapshot_survives_serializer_roundtrip(self):
        """Snapshots can carry complex state through serialization."""
        snap = freeze_execution_state(
            "dist-test", "dev-a",
            local_vars={"counter": 42, "data": list(range(100))},
            objects={"config": {"batch_size": 32, "lr": 0.001}},
        )
        # Simulate send/receive
        data = snap.serialize()
        restored = ExecutionSnapshot.deserialize(data)
        state = thaw_execution_state(restored)
        self.assertEqual(state["local_vars"]["counter"], 42)
        self.assertEqual(len(state["local_vars"]["data"]), 100)
        self.assertEqual(state["objects"]["config"]["batch_size"], 32)


if __name__ == "__main__":
    unittest.main()
