"""This is an airflow backfill helper tool.
"""

import logging
import os
import re
import subprocess
from datetime import timedelta

import click
from tqdm import tqdm

__copyright__ = "Copyright 2021 (C) Sebastian Schaetz"

try:
    from quick import gui_option

    @gui_option
    @click.group()
    @click.pass_context
    def cli(ctx):
        ctx.ensure_object(dict)
        logging.basicConfig(level=logging.INFO)


except ImportError:

    @click.group()
    @click.pass_context
    def cli(ctx):
        ctx.ensure_object(dict)
        logging.basicConfig(level=logging.INFO)


def hour_day_to_timedelta(s: str):
    p = re.compile(r"(\d{1,2}(H|h))|(\d{1,3}(D|d))")
    if p.match(s) is None:
        raise ValueError(f"'{s}' is not a valid string")
    s = s.upper()
    if s[-1] == "H":
        return timedelta(hours=int(s[0:-1]))
    if s[-1] == "D":
        return timedelta(days=int(s[0:-1]))


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


@cli.command(help="Clean and Backfill GCP Composer")
@click.pass_context
@click.option("-s", "--start_dt", type=click.DateTime())
@click.option("-e", "--end_dt", type=click.DateTime())
@click.option("-env", "--environment", type=str)
@click.option("-p", "--project", type=str)
@click.option("-l", "--location", type=str)
@click.option("-d", "--dag", type=str)
@click.option("-t", "--task-regex", type=str, default=None)
@click.option("-dr", "--dry_run", is_flag=True, help="Dry run.")
@click.option(
    "--interval",
    type=str,
    default="6h",
    help="Specify the interval to split up the timespan. "
    "Either hours (example: 4h) or days (example (1d).",
)
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
    interval,
):
    intervals = split_timespan(
        start_dt=start_dt,
        end_dt=end_dt,
        td=hour_day_to_timedelta(interval),
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
                "--",
                f"{dag}",
                f"-s {ival[0].strftime('%Y-%m-%dT%H:%M')}",
                f"-e {ival[1].strftime('%Y-%m-%dT%H:%M')}",
                "-y",
            ]
            if task_regex is not None:
                cmd.append(f'--task_regex "{task_regex}"')
            if af_cmd == "backfill" and dry_run:
                cmd.append("--dry_run")

            commands.append(" ".join(cmd).split(" "))

    for cmd in tqdm(commands):
        subprocess.run(cmd, env=os.environ.copy())


@cli.command(help="Clean and Backfill Airflow")
@click.pass_context
@click.option("-s", "--start_dt", type=click.DateTime())
@click.option("-e", "--end_dt", type=click.DateTime())
@click.option("-l", "--location", type=str)
@click.option("-t", "--task-regex", type=str, default=None)
@click.option("-dr", "--dry_run", is_flag=True, help="Dry run.")
@click.option(
    "--interval",
    type=str,
    default="6h",
    help="Specify the interval to split up the timespan. "
    "Either hours (example: 4h) or days (example (1d).",
)
def cb(
    ctx,
    start_dt,
    end_dt,
    dag,
    task_regex,
    dry_run,
    interval,
):
    intervals = split_timespan(
        start_dt=start_dt,
        end_dt=end_dt,
        td=hour_day_to_timedelta(interval),
    )

    commands = []
    for ival in intervals:
        for af_cmd in ["clear", "backfill"]:
            cmd = [
                "airflow",
                f"{af_cmd}",
                f"{dag}",
                f"-s {ival[0].strftime('%Y-%m-%dT%H:%M')}",
                f"-e {ival[1].strftime('%Y-%m-%dT%H:%M')}",
                "-y",
            ]
            if task_regex is not None:
                cmd.append(f'--task_regex "{task_regex}"')
            if af_cmd == "backfill" and dry_run:
                cmd.append("--dry_run")

            commands.append(" ".join(cmd).split(" "))

    for cmd in tqdm(commands):
        subprocess.run(cmd, env=os.environ.copy())


if __name__ == "__main__":
    cli(obj=dict())
