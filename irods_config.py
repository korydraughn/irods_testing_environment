# grown-up modules
import docker
import json
import logging
import os

# local modules
from irods_testing_environment import compose_cli
import context
import execute
import json_utils

if __name__ == "__main__":
    import argparse
    import logs

    import cli

    parser = argparse.ArgumentParser(description='Configure a running iRODS Zone for running tests.')

    cli.add_common_args(parser)
    cli.add_compose_args(parser)

    args = parser.parse_args()

    docker_client = docker.from_env()

    compose_project = compose_cli.get_project(
        project_dir=os.path.abspath(args.project_directory),
        project_name=args.project_name,
        docker_client=docker_client)

    logs.configure(args.verbosity)

    try:
        configure_irods_testing(docker_client, compose_project)

    except Exception as e:
        logging.critical(e)
        exit(1)
