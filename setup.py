import sys
import os
import shutil
from pathlib import Path
from setuptools import setup
from setuptools.command.test import test as TestCommand
from distutils.extension import Extension
from Cython.Build import cythonize
from Cython.Build.Dependencies import parse_dependencies

RECOMPILE = False

subcommand_args = []
if "--" in sys.argv:
    subcommand_args = sys.argv[sys.argv.index("--") + 1:]
    del sys.argv[sys.argv.index("--"):]

define_macros = {}
undef_macros = []
extra_compile_args = []

if sys.platform == "win32":
    DEVELOP = sys.executable.endswith("python_d.exe")

    if DEVELOP:
        define_macros["_DEBUG"] = "1"
        undef_macros.append("NDEBUG")
        extra_compile_args.append("/MTd")
        extra_compile_args.append("/Zi")
    else:
        undef_macros.append("_DEBUG")
else:
    DEVELOP = sys.executable.endswith("-dbg")
    extra_compile_args.append("-std=c++11")

    if DEVELOP:
        define_macros["_DEBUG"] = 1
        undef_macros.append("NDEBUG")
    else:
        extra_compile_args.append("-O3")

extensions = [
    Extension(
        "*",
        ["src/yapic/entity/**/*.pyx"],
        language="c++",
        include_dirs=["./libs/yapic.core/src/yapic/core/include"],
        extra_compile_args=extra_compile_args,
        define_macros=list(define_macros.items()),
        undef_macros=undef_macros,
    )
]


def update_deps():
    files = list(_cython_files(os.path.realpath("src/yapic/entity")))
    touch_deps(files, set())


def _cython_files(dir: str):
    for entry in os.listdir(dir):
        full_entry = os.path.join(dir, entry)
        if os.path.isdir(full_entry):
            for x in _cython_files(full_entry):
                yield x
        else:
            base, ext = os.path.splitext(full_entry)
            if ext in (".pyx", ".pxd"):
                yield full_entry, get_deps(full_entry)


def touch_deps(files, touched):
    for file, deps in files:
        if not deps:
            continue

        if file not in touched:
            deps_mtim = get_max_mtime(deps)
            file_mtim = os.path.getmtime(file)

            if file_mtim < deps_mtim:
                os.utime(file, (deps_mtim, deps_mtim))
                touched.add(file)

                for reverse in files:
                    if file in reverse[1]:
                        touch_deps([reverse], touched)


def get_deps(filename: str):
    result = set()
    base_dir, base_name = os.path.split(filename)
    cimports, includes, externs, distutils_info = parse_dependencies(filename)

    for cimp in cimports:
        if cimp.startswith("cpython"):
            continue
        if cimp[0] == ".":
            possible_entries = [
                os.path.join(base_dir, cimp[1:] + ".pyx"),
                os.path.join(base_dir, cimp[1:] + ".pxd"),
                os.path.join(base_dir, cimp[1:] + "pyx"),
                os.path.join(base_dir, cimp[1:] + "pxd"),
            ]
            for pe in possible_entries:
                if os.path.isfile(pe):
                    result.add(pe)

    return list(result)


def get_max_mtime(deps: list):
    return max(os.path.getmtime(p) for p in deps)


if RECOMPILE:
    shutil.rmtree(os.path.join(os.path.dirname(__file__), "build"), ignore_errors=True)
    update_deps()


def cmd_prerun(cmd: TestCommand, requirements):
    for r in requirements(cmd.distribution):
        installed = cmd.distribution.fetch_build_eggs(r if r else [])

        for dp in map(lambda x: x.location, installed):
            if dp not in sys.path:
                sys.path.insert(0, dp)

    # cmd.distribution.get_command_obj("build").force = True
    if RECOMPILE:
        cmd.run_command("build")

    ext = cmd.get_finalized_command("build_ext")
    ep = str(Path(ext.build_lib).absolute())

    if ep not in sys.path:
        sys.path.insert(0, ep)

    for e in ext.extensions:
        if e._needs_stub:
            ext.write_stub(ep, e, False)


class PyTest(TestCommand):
    user_options = [
        ("file=", "f", "File to run"),
    ]

    def initialize_options(self):
        super().initialize_options()
        self.pytest_args = "-x -s"
        self.file = "./tests/"

    def finalize_options(self):
        super().finalize_options()
        if self.file:
            self.pytest_args += " " + self.file.replace("\\", "/")

    def run(self):

        def requirements(dist):
            yield dist.install_requires
            yield dist.tests_require

        cmd_prerun(self, requirements)
        self.run_tests()

    def run_tests(self):
        import pytest
        errno = pytest.main(subcommand_args)
        sys.exit(errno)


almafa = setup(
    name="yapic.entity",
    version="1.0.0",
    packages=["yapic.entity", "yapic.entity.sql", "yapic.entity.sql.pgsql", "yapic.entity.sql.pgsql.postgis"],
    package_dir={"yapic.entity": "src/yapic/entity"},
    package_data={
        "yapic.entity.sql": ["_connection.pyi", "_query_context.pyi", "_query.pyi"],
        "yapic.entity.sql.pgsql": ["_connection.pyi"]
    },
    python_requires=">=3.7",
    ext_modules=cythonize(
        extensions,
        force=True,
        compiler_directives={
            "language_level": 3,
            "iterable_coroutine": False,
            "boundscheck": DEVELOP,
            "wraparound": False,
            "auto_pickle": False
        },
    ),
    install_requires=Path(__file__).parent.joinpath("requirements.txt").read_text().splitlines(),
    tests_require=["pytest", "docker", "pytest-asyncio", "pytest-leaks", "yapic.json"],
    cmdclass={"test": PyTest})
