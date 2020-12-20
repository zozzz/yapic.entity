import sys
from os import path

if sys.platform == "win32":
    DEVELOP = sys.executable.endswith("python_d.exe")
else:
    DEVELOP = sys.executable.endswith("-dbg")

if DEVELOP:
    sys.path.insert(0, path.join(path.dirname(__file__), "..", "build", "lib.win-amd64-3.9-pydebug"))
else:
    sys.path.insert(0, path.join(path.dirname(__file__), "..", "build", "lib.win-amd64-3.9"))
