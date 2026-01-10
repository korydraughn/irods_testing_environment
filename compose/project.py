"""Minimal compose Project implementation backed by Docker Compose CLI."""

import os
import shutil
import subprocess

import docker

from .container import Container

def _sanitize_project_name(name):
    # Match legacy usage in this repo: strip characters compose v1 rejects.
    return name.replace(".", "").replace(":", "").replace("/", "")

class Project:
    """Subset of compose.project.Project used by this codebase."""
    def __init__(self, project_dir, project_name=None, docker_client=None):
        self.project_dir = os.path.abspath(project_dir)
        base_name = os.path.basename(self.project_dir.rstrip(os.sep))
        name = project_name or base_name
        self.name = _sanitize_project_name(name)
        self._docker_client = docker_client or docker.from_env()

    def _compose_cmd(self, args):
        if not shutil.which("docker"):
            raise RuntimeError("docker CLI not found in PATH")
        cmd = ["docker", "compose", "-p", self.name]
        cmd.extend(args)
        subprocess.run(cmd, cwd=self.project_dir, check=True)

    def build(self):
        """Build the compose project images."""
        self._compose_cmd(["build"])

    def up(self, scale_override=None):
        """Start services with optional scale overrides, return containers."""
        args = ["up", "-d"]
        if scale_override:
            for service, count in scale_override.items():
                args.extend(["--scale", f"{service}={count}"])
        self._compose_cmd(args)
        return self.containers()

    def down(self, include_volumes=False, remove_image_type=False):
        """Stop and remove compose resources."""
        args = ["down"]
        if include_volumes:
            args.append("--volumes")
        if remove_image_type:
            args.extend(["--rmi", "all"])
        self._compose_cmd(args)

    def containers(self, service_names=None):
        """Return containers for this compose project."""
        label_filters = [f"com.docker.compose.project={self.name}"]
        if service_names:
            label_filters.extend([f"com.docker.compose.service={s}" for s in service_names])
        containers = self._docker_client.containers.list(all=True, filters={"label": label_filters})
        return [Container(c.name) for c in containers]
