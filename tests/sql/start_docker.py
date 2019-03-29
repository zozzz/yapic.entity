import os

SELF_PATH = os.path.realpath(os.path.dirname(__file__))
PORT = 5432

os.system(f"docker build --rm -t pgsql:yapic_entity \"{SELF_PATH}\"")
os.system(f"docker run --rm -p 5432:{PORT} -t pgsql:yapic_entity")
