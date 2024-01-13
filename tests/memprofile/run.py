import os
import shlex
import sys
from pathlib import Path
from subprocess import Popen

try:
    import yapic.entity
except ImportError:
    installed = False
else:
    installed = True

# TODO
# python = sys.executable
python = "python3"

# XXX: ha python3-dbg
# installed=$(python -c 'import pkgutil; print(1 if pkgutil.find_loader("yapic.entity") else 0)')

SELF = Path(__file__)
ROOT = SELF.parent.parent.parent
DIST = ROOT / "dist"
MEMRY_OUT = ROOT / "memray-out"


def cmd(cmd):
    print(f">>> {cmd}")
    envs = dict(os.environ)
    envs["MEMRAY"] = "1"
    proc = Popen(shlex.split(cmd), env=envs)
    res = proc.wait()

    # res = system(cmd)
    if res != 0:
        sys.exit(res)

def build():
    cmd(f"{python} setup.py bdist_wheel")

def whl():
    for entry in DIST.glob("*.whl"):
        return DIST / entry


def run_memray(file: Path, report = "tree"):
    out = MEMRY_OUT / file.relative_to(ROOT)
    out.unlink(missing_ok=True)
    out.parent.mkdir(parents=True, exist_ok=True)

    cmd(f"{python} -m memray run -o '{out}' --native '{file}'")
    cmd(f"python3 -m memray {report} '{out}'")


def main():
    if not installed:
        if not DIST.exists():
            build()

        if not whl():
            build()

        cmd(f"python3 -m pip install '{whl()}'")

    run_memray(SELF.parent / "main.py")

if __name__ == "__main__":
    main()
