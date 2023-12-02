import angreal

import os
import subprocess


cwd = os.path.join(angreal.get_root(), '..')
docker_compose = os.path.join(angreal.get_root(), '..', 'dev','docker-compose.yaml')
logs = os.path.join(angreal.get_root(), '..', 'dev','logs')

@angreal.command(name="demo-start", about="start services for example dags")
def demo_start():
    subprocess.run(
        	(f"docker-compose -f {docker_compose} build --no-cache && docker-compose -f {docker_compose} up -d"),
            shell=True,
            cwd=cwd
    )


@angreal.command(name="demo-stop", about="stop services for example dags")
def demo_stop():
    subprocess.run(
        	(f"docker-compose -f {docker_compose} down"),
            shell=True,
            cwd=cwd
    )


@angreal.command(name="demo-clean", about="shut down services and remove files")
def demo_clean():
    subprocess.run(
        	(f"docker-compose -f {docker_compose} down --volumes --remove-orphans", f"rm -rf {logs}"),
            shell=True,
            cwd=cwd
    )
