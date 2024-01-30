import os
import subprocess

from angreal.integrations.git import Git
from angreal.integrations.venv import VirtualEnv


def init():
    os.chdir("airflow-dev")
    VirtualEnv(".venv", now=True, requirements="dev_requirements.txt").install_requirements()

    g = Git()
    g.init()
    g.add('.')

    subprocess.run(
        (
        "pre-commit install;"
        "pre-commit run --all-files;"
        "pre-commit run --all-files;"
        ),
        shell=True,
    )

    g.commit("-am 'airflow-dev initialized via angreal'")
