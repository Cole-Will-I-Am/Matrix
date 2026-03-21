"""Tests for ghost_tap — passive signal interception layer."""

import time
import types
import threading
import unittest

from ghost_tap import GhostTap, Signal, SignalBuffer, quick_tap


# ─── Dummy targets ──────────────────────────────────────────────────────────

def _make_target_module():
    """Create a fake module with functions to tap."""
    mod = types.ModuleType("dummy_target")

    def add(a, b):
        return a + b

    def greet(name, greeting="hello"):
        return f"{greeting}, {name}"

    def slow_op():
        time.sleep(0.01)
        return "done"

    mod.add = add
    mod.greet = greet
    mod.slow_op = slow_op
    mod.__name__ = "dummy_target"
    return mod


class TestSignalBuffer(unittest.TestCase):

    def test_push_and_drain(self):
        buf = SignalBuffer(capacity=100)
        sig = Signal("t", 0.0, "()", "{}", "1", 0.0, 1)
        buf.push(sig)
        self.assertEqual(buf.count, 1)
        drained = buf.drain()
        self.assertEqual(len(drained), 1)
        self.assertEqual(buf.count, 0)

    def test_capacity_drops_oldest(self):
        buf = SignalBuffer(capacity=3)
        for i in range(5):
            buf.push(Signal("t", 0.0, str(i), "{}", "", 0.0, i))
        self.assertEqual(buf.count, 3)
        self.assertEqual(buf.total_dropped, 2)
        signals = buf.drain()
        # Should have the last 3
        self.assertEqual([s.call_seq for s in signals], [2, 3, 4])

    def test_peek_does_not_drain(self):
        buf = SignalBuffer(capacity=100)
        for i in range(5):
            buf.push(Signal("t", 0.0, "", "{}", "", 0.0, i))
        peeked = buf.peek(3)
        self.assertEqual(len(peeked), 3)
        self.assertEqual(buf.count, 5)  # still full

    def test_clear(self):
        buf = SignalBuffer(capacity=100)
        for i in range(10):
            buf.push(Signal("t", 0.0, "", "{}", "", 0.0, i))
        count = buf.clear()
        self.assertEqual(count, 10)
        self.assertEqual(buf.count, 0)

    def test_thread_safety(self):
        buf = SignalBuffer(capacity=1000)
        errors = []

        def writer(start):
            try:
                for i in range(100):
                    buf.push(Signal("t", 0.0, "", "{}", "", 0.0, start + i))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i * 100,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertEqual(buf.total_captured, 500)


class TestGhostTapInstall(unittest.TestCase):

    def test_install_and_capture(self):
        mod = _make_target_module()
        tap = GhostTap()

        ok = tap.install("add_tap", mod, "add")
        self.assertTrue(ok)

        # Call the tapped function — should still work normally
        result = mod.add(2, 3)
        self.assertEqual(result, 5)

        # Should have captured a signal
        self.assertEqual(tap.signal_count, 1)

        signals = tap.drain()
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].tap_name, "add_tap")
        self.assertIn("2", signals[0].args_repr)
        self.assertIn("5", signals[0].return_repr)

        tap.vanish()

    def test_install_duplicate_name_fails(self):
        mod = _make_target_module()
        tap = GhostTap()
        tap.install("t", mod, "add")
        self.assertFalse(tap.install("t", mod, "greet"))
        tap.vanish()

    def test_install_missing_attr_fails(self):
        mod = _make_target_module()
        tap = GhostTap()
        self.assertFalse(tap.install("t", mod, "nonexistent"))
        tap.vanish()

    def test_kwargs_captured(self):
        mod = _make_target_module()
        tap = GhostTap()
        tap.install("greet_tap", mod, "greet")

        result = mod.greet("world", greeting="hey")
        self.assertEqual(result, "hey, world")

        signals = tap.drain()
        self.assertIn("world", signals[0].args_repr)
        self.assertIn("hey", signals[0].kwargs_repr)
        tap.vanish()

    def test_duration_tracked(self):
        mod = _make_target_module()
        tap = GhostTap()
        tap.install("slow_tap", mod, "slow_op")

        mod.slow_op()
        signals = tap.drain()
        # Should be at least 10ms = 10000µs
        self.assertGreater(signals[0].duration_us, 5000)
        tap.vanish()


class TestGhostTapRemove(unittest.TestCase):

    def test_remove_restores_original(self):
        mod = _make_target_module()
        original_add = mod.add
        tap = GhostTap()
        tap.install("t", mod, "add")

        # Hooked version works
        self.assertEqual(mod.add(1, 2), 3)
        self.assertEqual(tap.signal_count, 1)

        # Remove tap
        tap.remove("t")
        mod.add(10, 20)
        # Should NOT capture after removal
        self.assertEqual(tap.signal_count, 1)

        tap.vanish()

    def test_remove_nonexistent(self):
        tap = GhostTap()
        self.assertFalse(tap.remove("ghost"))
        tap.vanish()


class TestGhostTapTransform(unittest.TestCase):

    def test_transform_args(self):
        mod = _make_target_module()
        tap = GhostTap()

        def double_first(args, kwargs):
            return (args[0] * 2, args[1]), kwargs

        tap.install("t", mod, "add", transform_args=double_first)
        result = mod.add(3, 4)
        self.assertEqual(result, 10)  # (3*2) + 4
        tap.vanish()

    def test_transform_return(self):
        mod = _make_target_module()
        tap = GhostTap()

        tap.install("t", mod, "add", transform_return=lambda r: r * 100)
        result = mod.add(2, 3)
        self.assertEqual(result, 500)  # 5 * 100
        tap.vanish()


class TestGhostTapVanish(unittest.TestCase):

    def test_vanish_clears_everything(self):
        mod = _make_target_module()
        tap = GhostTap()
        tap.install("t1", mod, "add")
        tap.install("t2", mod, "greet")

        mod.add(1, 2)
        mod.greet("x")

        stats = tap.vanish()
        self.assertEqual(stats["signals_buffered"], 2)
        self.assertEqual(tap.signal_count, 0)
        self.assertEqual(len(tap._taps), 0)

        # Functions should work normally after vanish
        self.assertEqual(mod.add(1, 1), 2)

    def test_context_manager(self):
        mod = _make_target_module()
        with GhostTap() as tap:
            tap.install("t", mod, "add")
            mod.add(1, 2)
            self.assertEqual(tap.signal_count, 1)
        # After exit, buffer is cleared
        self.assertEqual(tap.signal_count, 0)


class TestGhostTapStats(unittest.TestCase):

    def test_stats_structure(self):
        mod = _make_target_module()
        tap = GhostTap()
        tap.install("add_tap", mod, "add")
        mod.add(1, 2)
        mod.add(3, 4)

        stats = tap.stats
        self.assertEqual(stats["active_taps"], 1)
        self.assertEqual(stats["signals_buffered"], 2)
        self.assertEqual(stats["tap_details"]["add_tap"]["calls"], 2)
        self.assertTrue(stats["tap_details"]["add_tap"]["active"])
        tap.vanish()


class TestGhostTapDrainToBytes(unittest.TestCase):

    def test_drain_to_bytes(self):
        import gzip, json

        mod = _make_target_module()
        tap = GhostTap()
        tap.install("t", mod, "add")
        mod.add(1, 2)
        mod.add(3, 4)

        raw = tap.drain_to_bytes(label="test_batch")
        self.assertIsNotNone(raw)

        payload = json.loads(gzip.decompress(raw))
        self.assertEqual(payload["label"], "test_batch")
        self.assertEqual(payload["count"], 2)
        self.assertEqual(len(payload["signals"]), 2)

        tap.vanish()

    def test_drain_to_bytes_empty(self):
        tap = GhostTap()
        self.assertIsNone(tap.drain_to_bytes())
        tap.vanish()


class TestQuickTap(unittest.TestCase):

    def test_quick_tap_multiple(self):
        mod = _make_target_module()
        tap = quick_tap(mod, ["add", "greet"])

        mod.add(1, 2)
        mod.greet("test")

        self.assertEqual(tap.signal_count, 2)
        stats = tap.stats
        self.assertEqual(stats["active_taps"], 2)
        tap.vanish()


class TestGhostTapConcurrency(unittest.TestCase):

    def test_concurrent_calls(self):
        mod = _make_target_module()
        tap = GhostTap(buffer_capacity=5000)
        tap.install("t", mod, "add")

        errors = []

        def caller():
            try:
                for _ in range(100):
                    mod.add(1, 1)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=caller) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertEqual(tap.signal_count, 1000)
        tap.vanish()


class TestGhostTapRepr(unittest.TestCase):

    def test_repr(self):
        tap = GhostTap()
        r = repr(tap)
        self.assertIn("GhostTap", r)
        self.assertIn("taps=0", r)
        tap.vanish()


if __name__ == "__main__":
    unittest.main()
