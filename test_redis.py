"""
test_redis.py — Comprehensive test suite for mini_redis.py
Uses pytest + pytest-asyncio with a real server spun up per test session.

Install dependencies:
    pip install pytest pytest-asyncio

Run:
    pytest test_redis.py -v
"""

import asyncio
import pytest
import pytest_asyncio
from mini_redis import Client, Server, ProtocolHandler, CommandError, Error
from io import BytesIO


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def server():
    """
    Spin up a real Server instance once for the whole session on port 31338
    (avoids clashing with a live Redis or a running dev server on 31337).
    The server runs as a background task and is cancelled after all tests.
    """
    srv = Server(host="127.0.0.1", port=31338)
    task = asyncio.get_event_loop().create_task(srv.run())
    # Give the server a moment to bind and start accepting connections.
    await asyncio.sleep(0.1)
    yield srv
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest_asyncio.fixture
async def client(server):
    """
    Fresh Client for every test — guarantees a clean connection.
    The client is explicitly closed after each test.
    """
    c = Client(host="127.0.0.1", port=31338)
    await c.connect()
    yield c
    await c.close()


@pytest_asyncio.fixture(autouse=True)
async def flush_between_tests(server, client):
    """Flush the key-value store before every test for isolation."""
    await client.flush()
    yield


# ---------------------------------------------------------------------------
# Command Tests
# ---------------------------------------------------------------------------

class TestSetAndGet:
    async def test_set_returns_1(self, client):
        result = await client.set("key1", "value1")
        assert result == 1

    async def test_get_existing_key(self, client):
        await client.set("key1", "value1")
        assert await client.get("key1") == b"value1"

    async def test_get_missing_key_returns_none(self, client):
        assert await client.get("nonexistent") is None

    async def test_set_overwrites_existing_key(self, client):
        await client.set("key1", "first")
        await client.set("key1", "second")
        assert await client.get("key1") == b"second"

    async def test_set_and_get_integer_value(self, client):
        await client.set("counter", "42")
        assert await client.get("counter") == b"42"

    async def test_set_and_get_empty_string(self, client):
        await client.set("empty", "")
        assert await client.get("empty") == b""

    async def test_set_and_get_binary_data(self, client):
        await client.set("binkey", b"\x00\x01\x02\xff")
        assert await client.get("binkey") == b"\x00\x01\x02\xff"


class TestDelete:
    async def test_del_existing_key_returns_1(self, client):
        await client.set("key1", "value1")
        assert await client.delete("key1") == 1

    async def test_del_removes_key(self, client):
        await client.set("key1", "value1")
        await client.delete("key1")
        assert await client.get("key1") is None

    async def test_del_missing_key_returns_0(self, client):
        assert await client.delete("ghost") == 0

    async def test_del_idempotent_on_second_call(self, client):
        await client.set("key1", "value1")
        await client.delete("key1")
        assert await client.delete("key1") == 0


class TestFlush:
    async def test_flush_clears_all_keys(self, client):
        await client.set("a", "1")
        await client.set("b", "2")
        await client.flush()
        assert await client.get("a") is None
        assert await client.get("b") is None

    async def test_flush_on_empty_store(self, client):
        # Should not raise — OK or 0 both acceptable
        result = await client.flush()
        assert result is not None


class TestMget:
    async def test_mget_multiple_existing_keys(self, client):
        await client.set("k1", "v1")
        await client.set("k2", "v2")
        result = await client.mget("k1", "k2")
        assert result == [b"v1", b"v2"]

    async def test_mget_with_missing_key_returns_none_in_list(self, client):
        await client.set("k1", "v1")
        result = await client.mget("k1", "missing")
        assert result == [b"v1", None]

    async def test_mget_all_missing(self, client):
        result = await client.mget("x", "y", "z")
        assert result == [None, None, None]

    async def test_mget_single_key(self, client):
        await client.set("solo", "only")
        assert await client.mget("solo") == [b"only"]


class TestMset:
    async def test_mset_multiple_pairs(self, client):
        result = await client.mset("k1", "v1", "k2", "v2")
        assert result == 1  # success indicator

    async def test_mset_values_retrievable_via_get(self, client):
        await client.mset("k1", "v1", "k2", "v2")
        assert await client.get("k1") == b"v1"
        assert await client.get("k2") == b"v2"

    async def test_mset_overwrites_existing_keys(self, client):
        await client.set("k1", "original")
        await client.mset("k1", "updated")
        assert await client.get("k1") == b"updated"

    async def test_mset_then_mget_roundtrip(self, client):
        await client.mset("a", "1", "b", "2", "c", "3")
        result = await client.mget("a", "b", "c")
        assert result == [b"1", b"2", b"3"]


# ---------------------------------------------------------------------------
# Protocol Handler Tests — serialization / deserialization
# ---------------------------------------------------------------------------

class TestProtocolHandlerSerialization:
    """Unit-test the ProtocolHandler in isolation, no network involved."""

    def _serialize(self, data) -> bytes:
        """Helper: serialize data to bytes using _write."""
        handler = ProtocolHandler()
        buf = BytesIO()
        handler._write(buf, data)
        buf.seek(0)
        return buf.read()

    async def _deserialize(self, raw: bytes):
        """Helper: deserialize bytes back to a Python object."""
        handler = ProtocolHandler()
        reader = asyncio.StreamReader()
        reader.feed_data(raw)
        return await handler.handle_request(reader)

    def test_serialize_simple_string(self):
        raw = self._serialize("hello")
        assert raw == b"$5\r\nhello\r\n"

    def test_serialize_bytes(self):
        raw = self._serialize(b"world")
        assert raw == b"$5\r\nworld\r\n"

    def test_serialize_integer(self):
        raw = self._serialize(42)
        assert raw == b":42\r\n"

    def test_serialize_none(self):
        raw = self._serialize(None)
        assert raw == b"$-1\r\n"

    def test_serialize_error(self):
        raw = self._serialize(Error("ERR something went wrong"))
        assert raw == b"-ERR something went wrong\r\n"

    def test_serialize_list(self):
        raw = self._serialize(["a", "b"])
        assert raw == b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"

    def test_serialize_empty_list(self):
        raw = self._serialize([])
        assert raw == b"*0\r\n"

    def test_serialize_nested_list(self):
        raw = self._serialize(["x", [1, 2]])
        assert b"*2\r\n" in raw
        assert b":1\r\n" in raw

    def test_serialize_dict(self):
        raw = self._serialize({"key": "val"})
        assert raw.startswith(b"%1\r\n")

    def test_serialize_unrecognized_type_raises(self):
        handler = ProtocolHandler()
        buf = BytesIO()
        with pytest.raises(CommandError):
            handler._write(buf, object())

    async def test_deserialize_bulk_string(self):
        raw = b"$5\r\nhello\r\n"
        result = await self._deserialize(raw)
        assert result == b"hello"

    async def test_deserialize_integer(self):
        raw = b":100\r\n"
        result = await self._deserialize(raw)
        assert result == 100

    async def test_deserialize_null(self):
        raw = b"$-1\r\n"
        result = await self._deserialize(raw)
        assert result is None

    async def test_deserialize_error(self):
        raw = b"-ERR bad command\r\n"
        result = await self._deserialize(raw)
        assert isinstance(result, Error)
        assert "bad command" in result.message

    async def test_deserialize_array(self):
        raw = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        result = await self._deserialize(raw)
        assert result == [b"foo", b"bar"]

    async def test_deserialize_dict(self):
        raw = b"%1\r\n$1\r\nk\r\n$1\r\nv\r\n"
        result = await self._deserialize(raw)
        assert result == {b"k": b"v"}

    async def test_roundtrip_nested_structure(self):
        original = ["SET", "mykey", "myvalue"]
        raw = self._serialize(original)
        result = await self._deserialize(raw)
        assert result == [b"SET", b"mykey", b"myvalue"]

    async def test_deserialize_unknown_prefix_raises(self):
        from mini_redis import Disconnect
        handler = ProtocolHandler()
        reader = asyncio.StreamReader()
        reader.feed_data(b"?unknown\r\n")
        with pytest.raises(CommandError):
            await handler.handle_request(reader)

    async def test_deserialize_empty_data_raises_disconnect(self):
        from mini_redis import Disconnect
        handler = ProtocolHandler()
        reader = asyncio.StreamReader()
        reader.feed_eof()
        with pytest.raises(Disconnect):
            await handler.handle_request(reader)


# ---------------------------------------------------------------------------
# Error Handling & Edge Cases
# ---------------------------------------------------------------------------

class TestErrorHandling:
    async def test_unknown_command_raises_command_error(self, client):
        with pytest.raises(CommandError):
            await client.execute("BADCMD", "arg")

    async def test_get_after_delete_is_none(self, client):
        await client.set("tmp", "val")
        await client.delete("tmp")
        assert await client.get("tmp") is None

    async def test_large_value(self, client):
        big = "x" * 100_000
        await client.set("big", big)
        result = await client.get("big")
        assert len(result) == 100_000

    async def test_key_with_spaces_in_value(self, client):
        await client.set("spaced", "hello world")
        assert await client.get("spaced") == b"hello world"

    async def test_unicode_value(self, client):
        await client.set("unicode", "日本語テスト")
        result = await client.get("unicode")
        assert result.decode("utf-8") == "日本語テスト"

    async def test_numeric_string_key(self, client):
        await client.set("12345", "numerickey")
        assert await client.get("12345") == b"numerickey"

    async def test_overwrite_with_empty_value(self, client):
        await client.set("k", "something")
        await client.set("k", "")
        assert await client.get("k") == b""

    async def test_mset_odd_args_is_handled(self, client):
        """MSET with odd number of args — server should either error or ignore last."""
        try:
            await client.mset("k1", "v1", "orphan_key")
        except (CommandError, IndexError):
            pass  # Either behaviour is acceptable; just must not crash the server.
        # Server should still be alive
        assert await client.get("k1") == b"v1"


# ---------------------------------------------------------------------------
# Concurrency / Multiple Clients
# ---------------------------------------------------------------------------

class TestConcurrency:
    async def test_two_clients_independent_connections(self, server):
        c1 = Client(host="127.0.0.1", port=31338)
        c2 = Client(host="127.0.0.1", port=31338)
        await c1.connect()
        await c2.connect()

        await c1.set("shared", "from_c1")
        result = await c2.get("shared")
        assert result == b"from_c1"

        await c1.close()
        await c2.close()

    async def test_concurrent_sets_from_multiple_clients(self, server):
        """Fire 20 concurrent SET commands across 10 clients; verify all keys land."""
        async def write_keys(client_id: int):
            c = Client(host="127.0.0.1", port=31338)
            await c.connect()
            for i in range(2):
                await c.set(f"c{client_id}_k{i}", f"v{client_id}{i}")
            await c.close()

        await asyncio.gather(*[write_keys(i) for i in range(10)])

        verifier = Client(host="127.0.0.1", port=31338)
        await verifier.connect()
        for i in range(10):
            for j in range(2):
                val = await verifier.get(f"c{i}_k{j}")
                assert val == f"v{i}{j}".encode()
        await verifier.close()

    async def test_concurrent_reads_do_not_interfere(self, server):
        """Write once, then read the same key from many concurrent clients."""
        writer = Client(host="127.0.0.1", port=31338)
        await writer.connect()
        await writer.set("readonly", "stable_value")
        await writer.close()

        async def read_key():
            c = Client(host="127.0.0.1", port=31338)
            await c.connect()
            val = await c.get("readonly")
            await c.close()
            return val

        results = await asyncio.gather(*[read_key() for _ in range(20)])
        assert all(r == b"stable_value" for r in results)

    async def test_server_handles_client_disconnect_gracefully(self, server):
        """Abruptly close a client mid-session; server should keep running."""
        rogue = Client(host="127.0.0.1", port=31338)
        await rogue.connect()
        await rogue.set("before_disconnect", "yes")
        # Close without proper shutdown
        rogue._writer.close()

        # Give the server a tick to handle the disconnect
        await asyncio.sleep(0.05)

        # A new well-behaved client should still work
        good = Client(host="127.0.0.1", port=31338)
        await good.connect()
        assert await good.get("before_disconnect") == b"yes"
        await good.close()


# ---------------------------------------------------------------------------
# Network & Connection Lifecycle (End-to-End)
# ---------------------------------------------------------------------------

class TestConnectionLifecycle:
    async def test_client_can_reconnect_after_close(self, server):
        c = Client(host="127.0.0.1", port=31338)
        await c.connect()
        await c.set("persist", "value")
        await c.close()

        # Reconnect and verify data is still there
        c2 = Client(host="127.0.0.1", port=31338)
        await c2.connect()
        assert await c2.get("persist") == b"value"
        await c2.close()

    async def test_multiple_commands_on_single_connection(self, server):
        """A single connection should handle many sequential commands reliably."""
        c = Client(host="127.0.0.1", port=31338)
        await c.connect()

        for i in range(50):
            await c.set(f"seq_{i}", str(i))
        for i in range(50):
            val = await c.get(f"seq_{i}")
            assert val == str(i).encode()

        await c.close()

    async def test_pipelining_style_mget_after_mset(self, server):
        """MSET followed immediately by MGET on the same connection."""
        c = Client(host="127.0.0.1", port=31338)
        await c.connect()

        keys = [f"pipe_{i}" for i in range(10)]
        flat_pairs = [item for i in range(10) for item in (f"pipe_{i}", f"pval_{i}")]

        await c.mset(*flat_pairs)
        result = await c.mget(*keys)
        assert result == [f"pval_{i}".encode() for i in range(10)]

        await c.close()

    async def test_server_reachable_after_flush(self, server):
        c = Client(host="127.0.0.1", port=31338)
        await c.connect()
        await c.set("before", "here")
        await c.flush()
        assert await c.get("before") is None
        # Server still alive and accepting new writes
        await c.set("after", "also_here")
        assert await c.get("after") == b"also_here"
        await c.close()