"""Shim for compose.cli.command.get_project used by this repo."""

from ..project import Project

def get_project(project_dir=None, project_name=None, **kwargs):
    """Return a Project compatible with the legacy docker-compose API."""
    if not project_dir:
        raise ValueError("project_dir is required")
    return Project(project_dir=project_dir, project_name=project_name)
