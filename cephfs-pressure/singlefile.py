import click
import yaml
import time
from socket import gaierror
from fabric.group import ThreadingGroup

@click.command()
@click.option("mountfile",   "-m", default="./mounts.yaml",     help="file (fqfp) to read mounts to write to")
@click.option("hostfile",    "-h", default="./inventory.yaml",  help="file (fqfp) to read hosts to operate on")
@click.option("--blocksize", "-b", default=4,                   help="size (KiB) of each I/O block")
@click.option("--size",      "-s", default=1,                   help="size (GiB) of the single target file per mount")
@click.option("--ttl",       "-t", default=30,                  help="time (seconds) to run I/O pressure for")
@click.option("--numjobs",   "-n", default=16,                  help="num  (int) of concurrent workers all targeting the same file per mount")
@click.option("--iodepth",   "-i", default=32,                  help="num  (int) of async I/O requests queued per worker")
@click.option("--debug",     "-d", is_flag=True, default=False, help="flag (bool) controlling log verboseness")
@click.option("--json",      "-j", is_flag=True, default=False, help="flag (bool) controlling output format")
def main(
  mountfile: str, hostfile: str,
  blocksize: int,
  size: int, ttl: int,
  numjobs: int, iodepth: int,
  debug: bool, json: bool,
):
  """Apply pressure to a a single file on a CephFS filesystem :)

  This tool allows the orchestration of lots of concurrent R/W operations acroos
  a set of hosts on a set of mounts.
  """

  # load our mount / hostfiles and
  # setup other runtime vars
  with open(hostfile, 'r') as filehandle:
    struct = yaml.safe_load(filehandle)
    host_list = []
    for group in struct.values():
      for hostname in group:
        host_list.append(hostname)

  with open(mountfile, 'r') as filehandle:
    struct = yaml.safe_load(filehandle)
    mount_list = []
    for group in struct.values():
      for mount in group:
        mount_list.append(mount)
    mount_precheck = " && ".join([f"[ -d '{mount}' ]" for mount in mount_list])
  
  PRECHECKS = [
    'echo $(uname -s) $(hostname)',
    'test -d "/etc/ceph/"',
    f'{mount_precheck}',
  ]
  
  if debug:
    print(
      f"prechecks: {PRECHECKS}         \n\n"
      f"mounts: {mount_list}           \n\n"
      f"hosts: {host_list}             \n\n"
    )

  # build our fio command to execute
  # on the targets.
  fiofile = f"{blocksize}k-randrw-singlefile"
  if json:
    fiofile += ".json"
  else:
    fiofile += ".txt"

  fio_cmd = (
    f"fio "
    f"--direct=1 --ioengine=libaio "
    f"--bs={blocksize}k --iodepth={iodepth} "
    f"--size={size}G --runtime={ttl} --time_based "
    f"--numjobs={numjobs} "
    f"--group_reporting "
    f"--fallocate=posix "
    f"--rw=randrw --rwmixread=50 "
    f"--output={fiofile} "
  )
  if json:
    fio_cmd += "--output-format=json "
  for jobnum, mount in enumerate(mount_list):
    target = f"{mount}/{fiofile}"
    fio_cmd += (
      f"--name=job-{jobnum} "
      f"--filename={target} "
    )

  if debug:
    print(
      f"fio cmd: {fio_cmd} \n\n"
      f"fiofile: {fiofile} \n\n"
    )

  # run prechecks and fio command
  # on the target hosts then pull
  # the fiolog per host
  with ThreadingGroup(*host_list) as targets:
    try:
      for precheck_cmd in PRECHECKS:
        targets.run(precheck_cmd, hide=(not debug))
    except gaierror:
      print(f"A host appears unreachable, check FQDN validity.")
      quit(1)
    except Exception as err:
      print(f"unexpected exception with '{precheck_cmd}': {err}")
      quit(1)

    try:
      targets.run(fio_cmd, hide=(not debug))
      time.sleep(ttl + 10)  # be patient! give fio time to flush :)
    except Exception as err:
      print(f"unexpected exception with '{fio_cmd}': {err}")
      quit(1)

    pathdir = f"./runs/run-{'json' if json else 'txt'}-singlefile-{blocksize}k-{time.time()}/"
    targets.get(fiofile, local=f"{pathdir}" + "{host}/")
    targets.run(f"rm -f {fiofile}", hide=(not debug))
    print(f"results in {pathdir}")

if __name__ == '__main__':
  main()
  exit(0)  # explicit not implicit :)
