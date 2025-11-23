# grown-up modules
import logging
import os
import re
import shlex
import shutil
import subprocess
from typing import Dict, Iterable, List, Optional


class ComposeError(RuntimeError):
    """Raised when the Compose CLI cannot be located or a command fails."""


def _split_command(command: str) -> List[str]:
    return shlex.split(command)


def _detect_compose_command() -> List[str]:
    """Return the preferred Compose CLI command."""
    env_override = os.environ.get('ITE_DOCKER_COMPOSE_COMMAND')
    if env_override:
        cmd = _split_command(env_override)
        logging.info('Using Compose command from ITE_DOCKER_COMPOSE_COMMAND: %s', ' '.join(cmd))
        return cmd

    detection_order = [
        ['docker', 'compose'],
        ['docker-compose'],
    ]

    for candidate in detection_order:
        executable = candidate[0]
        if shutil.which(executable) is None:
            continue

        try:
            logging.debug('Attempting Compose command detection with %s', ' '.join(candidate))
            subprocess.run(
                candidate + ['version'],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logging.info('Detected Compose command: %s', ' '.join(candidate))
            return candidate
        except (subprocess.CalledProcessError, FileNotFoundError):
            logging.debug('Compose command %s is unavailable.', ' '.join(candidate))
            continue

    raise ComposeError(
        'Unable to find a working Docker Compose CLI. Install the Docker Compose V2 '
        'plugin (`docker compose`) or the standalone `docker-compose` binary.'
    )


def _normalize_project_name(project_dir: str, project_name: Optional[str]) -> str:
    raw_name = (
        project_name
        or os.environ.get('COMPOSE_PROJECT_NAME')
        or os.path.basename(os.path.abspath(project_dir))
        or 'default'
    )
    return re.sub(r'[^-_a-z0-9]', '', raw_name.lower()) or 'default'


class ComposeProject:
    """Minimal wrapper for interacting with a Compose project via the CLI."""

    def __init__(
        self,
        docker_client,
        project_dir: str,
        project_name: Optional[str] = None,
        compose_command: Optional[Iterable[str]] = None,
    ):
        self.docker_client = docker_client
        self.project_directory = os.path.abspath(project_dir)
        self.name = _normalize_project_name(self.project_directory, project_name)
        if compose_command:
            self._compose_command = list(compose_command)
        else:
            self._compose_command = _detect_compose_command()
        logging.info(
            'Initialized Compose project [%s] in [%s] using command [%s]',
            self.name,
            self.project_directory,
            ' '.join(self._compose_command),
        )

    def _command(self, extra_args: Iterable[str]) -> List[str]:
        base_args = [
            '--project-directory',
            self.project_directory,
            '--project-name',
            self.name,
        ]
        return list(self._compose_command) + base_args + list(extra_args)

    def _run(self, extra_args: Iterable[str]) -> None:
        command = self._command(extra_args)
        readable_command = ' '.join(shlex.quote(c) for c in command)
        logging.info('Executing Compose command: %s', readable_command)

        result = subprocess.run(
            command,
            cwd=self.project_directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if result.stdout:
            logging.debug(result.stdout.strip())

        if result.returncode != 0:
            if result.stderr:
                logging.error(result.stderr.strip())
            raise ComposeError(f'Compose command failed: {" ".join(command)}')

    def build(self, service_names: Optional[Iterable[str]] = None) -> None:
        args: List[str] = ['build']
        target = ','.join(service_names) if service_names else 'all services'
        logging.info('Building Compose project [%s] (%s)', self.name, target)
        if service_names:
            args.extend(service_names)
        self._run(args)

    def up(self, scale_override: Optional[Dict[str, int]] = None) -> List:
        args: List[str] = ['up', '--detach']
        logging.info('Starting Compose project [%s]', self.name)
        if scale_override:
            for service, scale in scale_override.items():
                if scale is None:
                    continue
                logging.info('Scaling service [%s] to [%s]', service, scale)
                args.extend(['--scale', f'{service}={scale}'])
        self._run(args)
        return self.containers()

    def down(self, include_volumes: bool = False, remove_image_type=False) -> None:
        args: List[str] = ['down']
        logging.info(
            'Stopping Compose project [%s] (volumes=%s, remove_images=%s)',
            self.name,
            include_volumes,
            remove_image_type,
        )
        if include_volumes:
            args.append('--volumes')
        if isinstance(remove_image_type, str) and remove_image_type in ('local', 'all'):
            args.extend(['--rmi', remove_image_type])
        self._run(args)

    def containers(self, service_names: Optional[Iterable[str]] = None, stopped: bool = False) -> List:
        logging.info(
            'Listing containers for Compose project [%s] (services=%s, include_stopped=%s)',
            self.name,
            ','.join(service_names) if service_names else 'all',
            stopped,
        )
        filters = {'label': [f'com.docker.compose.project={self.name}']}
        containers = self.docker_client.containers.list(all=stopped, filters=filters)
        if service_names:
            allowed = set(service_names)
            containers = [
                c for c in containers
                if c.labels and c.labels.get('com.docker.compose.service') in allowed
            ]
        return containers


def get_project(
    project_dir: str,
    project_name: Optional[str] = None,
    docker_client=None,
    compose_command: Optional[Iterable[str]] = None,
) -> ComposeProject:
    """Return a ComposeProject backed by the Compose CLI."""
    if docker_client is None:
        import docker  # Local import to avoid unconditional dependency when unused.

        docker_client = docker.from_env()

    return ComposeProject(
        docker_client,
        project_dir,
        project_name=project_name,
        compose_command=compose_command,
    )
