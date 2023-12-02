import angreal
from angreal.integrations.venv import VirtualEnv

import os
import subprocess

venv_location = os.path.join(angreal.get_root(),'..','.venv')
cwd = os.path.join(angreal.get_root(), '..')

dev = angreal.command_group(name="dev",about="commands for your development environment")

@dev()
@angreal.command(name='setup', about="setup a development environment")
def setup_env():
    VirtualEnv(venv_location, now=True, requirements=".[dev]").install_requirements()
    subprocess.run(
        (
        "pre-commit install;"
        "pre-commit run --all-files;"
        ),
        shell=True,
        cwd=cwd
    )
