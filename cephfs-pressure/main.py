
import click
import yaml
import uuid
import json
import time
from os import mkdir
from socket import gaierror
from fabric import Connection
from fabric.group import ThreadingGroup

@click.command()
@click.option("mountfile",   "-m", default="./mounts.yaml",     help="file (fqfp) to read mounts to write to")
@click.option("hostfile",    "-h", default="./inventory.yaml",  help="file (fqfp) to read hosts to operate on")
@click.option("--blocksize", "-b", default=4,                   help="size (Byte) to write data blocks with fio")
@click.option("--fio-op",    "-f", default="randwrite",         help="Op   (String) to perform with fio (randwrite, randread, randrw)")
@click.option("--size",      "-s", default=1,                   help="size (GiB) total to write as part of a job")
@click.option("--ttl",       "-t", default=5,                   help="time (seconds) to allow the cmd to run for")
@click.option("--filenum",   "-f", default=5,                   help="num  (int) of files to create")
@click.option("--debug",     "-d", is_flag=True, default=False, help="flag (bool) controlling log verboseness")
@click.option("--json",      "-j", is_flag=True, default=False, help="flag (bool) controlling log format")

def main(
  mountfile: str, hostfile: str, 
  blocksize: int, fio_op: str, 
  size: int, ttl: int, filenum: int,
  debug: bool, json: bool,
):
  """Apply pressure to a CephFS filesystem :)

  This tool allows for the orchestration of pressure testing with FIO across 
  hosts or containers from a given bastion host, while also collecting runtime
  info for each for later processing.
  """

  # load our mount / hostfiles and
  # setup other runtime vars
  with open(hostfile, 'r') as filehandle:
    struct = yaml.safe_load(filehandle)
    host_list = []
    for group in struct.values():
      for hostname in group:
        host_list.append(hostname)
  
  with open (mountfile, 'r') as filehandle:
    struct = yaml.safe_load(filehandle)
    mount_list = []
    for group in struct.values():
      for mount in group:
        mount_list.append(mount)
    mount_precheck = " && ".join([f"[ -d '{mount}' ]" for mount in mount_list])

  VALID_FIO_OPS = [
    'randwrite',
    'randread',
    'randrw'
  ]
  PRECHECKS = [
    'echo $(uname -s) $(hostname)',
    'test -d "/etc/ceph/"',
    f'{mount_precheck}', 
  ]

  if debug:
    print(
      f"Valid fio ops: {VALID_FIO_OPS} \n\n"
      f"prechecks: {PRECHECKS}         \n\n"
      f"mounts: {mount_list}           \n\n"
      f"hosts: {host_list}             \n\n"
    )


  # build our fio command to execute
  # on the targets.
  fiofile = f"/tmp/{blocksize}k-{fio_op}"
  if json:
    fiofile += ".json"
  else:
    fiofile += ".txt"

  fio_cmd = (
    f"fio "
    f"--direct=1 --ioengine=libaio "
    f"--unlink=1 "
    f"--bs={blocksize}k --iodepth=32 "
    f"--size {size}G --runtime {ttl} "
    f"--group_reporting=0 "
    f"--output={fiofile} "
    f"--rw={fio_op} "
  )
  if fio_op == "randrw":
    fio_cmd += " --rwmixread=50 "
  if json:
    fio_cmd += f"--output-format=json "
  for jobnum, mount in enumerate(mount_list):
    fio_cmd += f"--name=job-{jobnum} "
    fio_cmd += f"--directory={mount} " 
    fio_cmd += f"--nrfiles={filenum} "

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
      print(f"'{trgt}' appears unreachable, check FQDN validity.")
      quit(1)
    except Exception as err:
      print(f"unexpected exception with '{precheck_cmd}': {err}")
      quit(1)

    try:
      targets.run(fio_cmd, hide=(not debug))
      time.sleep(ttl+10) # be paitent! give fio time :)
    except Exception as err:
      print(f"unexpected exception with '{fio_cmd}': {err}")
      quit(1)

    pathdir = f"./runs/run-{'json' if json else 'txt'}-{blocksize}k-{time.time()}/"
    targets.get(fiofile, local=f"{pathdir}"+"{host}/")
    targets.run(f"rm -f {fiofile}", hide=(not debug))
    print(f"results in {pathdir}")
    
if __name__ == '__main__':
  main()
  exit(0) # explicit not implicit :)
