import os

SELF_PATH = os.path.realpath(os.path.dirname(__file__))


def start_docker(port):
    os.system(f"docker build --rm -t pgsql:yapic_entity \"{SELF_PATH}\"")
    os.system(f"docker run --rm -p 5432:{port} -t pgsql:yapic_entity")


if __name__ == "__main__":
    start_docker(5432)
