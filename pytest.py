import os
import sys
import shlex

args = ""
if len(sys.argv) > 1:
    args = "-- " + " ".join(sys.argv[1:])

os.system("%s %s %s" % (sys.executable, "setup.py test", args))
