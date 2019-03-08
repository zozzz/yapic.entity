import sys
import glob
from pathlib import Path
from setuptools import setup
from setuptools.command.test import test as TestCommand
from distutils.extension import Extension
from Cython.Build import cythonize

extensions = [
    Extension("*", ["src/yapic/entity/*.pyx"], language="c++"),
]

subcommand_args = []
if "--" in sys.argv:
    subcommand_args = sys.argv[sys.argv.index("--") + 1:]
    del sys.argv[sys.argv.index("--"):]


def cmd_prerun(cmd: TestCommand, requirements):
    for r in requirements(cmd.distribution):
        installed = cmd.distribution.fetch_build_eggs(r if r else [])

        for dp in map(lambda x: x.location, installed):
            if dp not in sys.path:
                sys.path.insert(0, dp)

    cmd.distribution.get_command_obj("build").force = True
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
        import shlex
        import pytest
        errno = pytest.main(subcommand_args)
        sys.exit(errno)


almafa = setup(
    name="yapic.entity",
    packages=["yapic.entity"],
    package_dir={"yapic.entity": "src/yapic/entity"},
    python_requires=">=3.7",
    ext_modules=cythonize(
        extensions,
        force=True,
        compiler_directives={
            "language_level": 3,
            "iterable_coroutine": False,
            "boundscheck": False,
            "wraparound": False
        }),
    tests_require=["pytest", "typing_inspect"],
    cmdclass={"test": PyTest})
