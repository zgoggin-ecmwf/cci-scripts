import click
import asyncio
import yaml
from concurrent.futures import ThreadPoolExecutor
from prometheus_client import generate_latest

from utils import probe_fs, registry_for_host, get_connection


def load_inventory(path: str):
  with open(path, "r") as f:
    data = yaml.safe_load(f)

  items = []
  for group, hosts in data.items():
    for host, attrs in hosts.items():
      for mount in attrs.get("mounts", []):
        items.append((host, group, mount))
  return items


async def _async_main(
  inventory_fqfp: str,
  fsize: int,
  textfile_dir: str,
  textfile_name: str,
  debug: bool
):
  loop = asyncio.get_running_loop()
  loop.set_default_executor(ThreadPoolExecutor(max_workers=32))

  targets = load_inventory(inventory_fqfp)

  tasks = []
  for host, group, mount in targets:
    tasks.append(
      probe_fs(
        host,
        group,
        mount,
        fsize,
        debug
      )
    )
  results = await asyncio.gather(*tasks)

  per_host = {}
  for result in results:
    per_host.setdefault(result["host"], []).append(result)

  for host, host_results in per_host.items():
    registry = registry_for_host(host_results)
    promified = generate_latest(registry).decode("utf-8")
    if debug:
      print(host, "\n", promified, "\n\n")

    local_tmp = "/tmp/cephfs_availability.prom"
    with open(local_tmp, "w") as f:
      f.write(promified)

    remote_tmp = "/tmp/cephfs_availability.prom"
    remote_final = f"{textfile_dir}/{textfile_name}"

    async def _upload():
      conn = await get_connection(host)
      await asyncio.to_thread(conn.put, local_tmp, remote_tmp)
      await asyncio.to_thread(
        conn.run,
        f"sudo chown root:root {remote_tmp}",
        hide=(not debug)
      )
      await asyncio.to_thread(
        conn.run,
        f"sudo mv {remote_tmp} {remote_final}",
        hide=(not debug)
      )
    await _upload()


@click.command()
@click.option("--inventory_fqfp", "-i", default="./inventory.yaml")
@click.option("--fsize", "-f", default=1024)
@click.option("--textfile_dir", "-m", default="/var/lib/node_exporter/textfile_collector")
@click.option("--textfile_name", "-n", default="cephfs_availability.prom")
@click.option("--debug", "-d", is_flag=True, default=False)
def main(
  inventory_fqfp: str,
  fsize: int,
  textfile_dir: str,
  textfile_name: str,
  debug: bool
):
  asyncio.run(
    _async_main(
      inventory_fqfp,
      fsize,
      textfile_dir,
      textfile_name,
      debug
    )
  )


if __name__ == "__main__":
  main()
  exit(0)
