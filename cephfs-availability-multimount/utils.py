import asyncio
import time
import uuid
import threading
from fabric import Connection
from prometheus_client import CollectorRegistry, Gauge

# persistent SSH connections per host
_CONNECTIONS = {}

# per-host locks so only one channel at a time per host
_HOST_LOCKS = {}

# limit concurrent SSH handshakes globally
_SSH_CONNECT_SEMAPHORE = asyncio.Semaphore(8)

def _is_alive(conn: Connection):
  try:
    return conn.transport is not None and conn.transport.is_active()
  except Exception:
    return False

def _new_connection(host: str):
  return Connection(host)

def _get_host_lock(host: str):
  if host not in _HOST_LOCKS:
    _HOST_LOCKS[host] = threading.Lock()
  return _HOST_LOCKS[host]

def _get_or_create_connection_sync(host: str):
  if host in _CONNECTIONS and _is_alive(_CONNECTIONS[host]):
    return _CONNECTIONS[host]
  conn = _new_connection(host)
  _CONNECTIONS[host] = conn
  return conn

async def get_connection(host: str):
  if host in _CONNECTIONS and _is_alive(_CONNECTIONS[host]):
    return _CONNECTIONS[host]

  async with _SSH_CONNECT_SEMAPHORE:
    return await asyncio.to_thread(_get_or_create_connection_sync, host)


def _run_ssh_sync(host: str, cmd: str, debug: bool):
  """Sync SSH execution, one channel at a time per host."""
  lock = _get_host_lock(host)
  with lock:
    conn = _get_or_create_connection_sync(host)
    try:
      return conn.run(cmd, hide=(not debug), in_stream=False)
    except Exception:
      _CONNECTIONS.pop(host, None)
      conn = _get_or_create_connection_sync(host)
      return conn.run(cmd, hide=(not debug), in_stream=False)


async def run_ssh(host: str, cmd: str, debug: bool):
  return await asyncio.to_thread(_run_ssh_sync, host, cmd, debug)


async def probe_fs(
  host: str, group: str, mount: str,
  fsize: int, debug: bool
):
  result = {
    "host": host,
    "group": group,
    "mount": mount,
    "up": 0,
    "write_latency": -1.0,
    "stat_latency": -1.0,
    "modify_latency": -1.0,
    "delete_latency": -1.0,
  }

  filename = f".probe-fs{uuid.uuid4().hex}"
  path = f"{mount}/{filename}"

  try:
    await run_ssh(host, f"test -d '{mount}'", debug)
    result["up"] = 1

    t = time.time()
    await run_ssh(host, f"head -c {fsize} /dev/urandom > '{path}' && sync", debug)
    result["write_latency"] = time.time() - t

    t = time.time()
    await run_ssh(host, f"stat '{path}'", debug)
    result["stat_latency"] = time.time() - t

    t = time.time()
    await run_ssh(host, f"head -c {fsize} /dev/urandom > '{path}' && sync", debug)
    result["modify_latency"] = time.time() - t

    t = time.time()
    await run_ssh(host, f"rm '{path}' && sync", debug)
    result["delete_latency"] = time.time() - t

  except Exception as err:
    print(f"[{host}] {err}")
  return result


def registry_for_host(results_for_host):
  registry = CollectorRegistry()

  g_up = Gauge(
    "cephfs_availability_up",
    "CephFS availability",
    ["host", "mount", "group"],
    registry=registry
  )
  g_write = Gauge(
    "cephfs_availability_write_latency_seconds",
    "Write latency",
    ["host", "mount", "group"],
    registry=registry
  )
  g_stat = Gauge(
    "cephfs_availability_stat_latency_seconds",
    "Stat latency",
    ["host", "mount", "group"],
    registry=registry
  )
  g_modify = Gauge(
    "cephfs_availability_modify_latency_seconds",
    "Modify latency",
    ["host", "mount", "group"],
    registry=registry
  )
  g_delete = Gauge(
    "cephfs_availability_delete_latency_seconds",
    "Delete latency",
    ["host", "mount", "group"],
    registry=registry
  )

  for r in results_for_host:
    host = r["host"]
    mount = r["mount"]
    group = r["group"]

    g_up.labels(host, mount, group).set(r["up"])
    g_write.labels(host, mount, group).set(r["write_latency"])
    g_stat.labels(host, mount, group).set(r["stat_latency"])
    g_modify.labels(host, mount, group).set(r["modify_latency"])
    g_delete.labels(host, mount, group).set(r["delete_latency"])

  return registry
