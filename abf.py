"""This is an airflow backfill helper tool.

"""

import logging
import os
import subprocess
from datetime import timedelta

import click
from tqdm import tqdm

__copyright__ = "Copyright 2021 (C) Sebastian Schaetz"


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)
    logging.basicConfig(level=logging.INFO)


def split_timespan(start_dt, end_dt, td):
    """Split a timespan identified by start_tt and end end_dt into
    smaller timespans of length td. The last timespan can be smaller than td.
    """

    intervals = []
    assert start_dt < end_dt, "start must be before end"
    cur_start_dt = start_dt
    while True:
        cur_stop_dt = cur_start_dt + td
        if cur_stop_dt > end_dt:
            cur_stop_dt = end_dt
        intervals.append([cur_start_dt, cur_stop_dt])
        if cur_stop_dt == end_dt:
            break
        cur_start_dt = cur_stop_dt
    return intervals


@cli.command(help="clean and backfill GCP Composer")
@click.pass_context
@click.option("-s", "--start_dt", type=click.DateTime())
@click.option("-e", "--end_dt", type=click.DateTime())
@click.option("-env", "--environment", type=str)
@click.option("-p", "--project", type=str)
@click.option("-l", "--location", type=str)
@click.option("-d", "--dag", type=str)
@click.option("-t", "--task-regex", type=str, default=None)
@click.option("-dr", "--dry_run", is_flag=True, help="Dry run.")
def cb_gcp(
    ctx,
    start_dt,
    end_dt,
    project,
    environment,
    location,
    dag,
    task_regex,
    dry_run,
):
    # TODO: add execution interval; currently defaults to 6 hours

    intervals = split_timespan(
        start_dt=start_dt, end_dt=end_dt, td=timedelta(hours=6),
    )

    commands = []
    for ival in intervals:
        for af_cmd in ["clear", "backfill"]:
            cmd = [
                "gcloud composer environments run",
                f"{environment}",
                f"--project {project}",
                f"--location {location}",
                f"{af_cmd}",
                f"--",
                f"{dag}",
                f"-s {ival[0].strftime('%Y-%m-%dT%H:%M')}",
                f"-e {ival[1].strftime('%Y-%m-%dT%H:%M')}",
                f"-y",
            ]
            if task_regex is not None:
                cmd.append(f'--task_regex "{task_regex}"')
            if af_cmd == "backfill" and dry_run:
                cmd.append("--dry_run")

            commands.append(" ".join(cmd).split(" "))

    for cmd in tqdm(commands):
        subprocess.run(cmd, env=os.environ.copy())
        print(" ".join(cmd))


if __name__ == "__main__":
    cli(obj=dict())
