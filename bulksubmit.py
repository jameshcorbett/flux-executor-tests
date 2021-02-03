#!/usr/bin/env python3
"""Script for submitting jobs to Flux."""

import flux
import sys
import os
import time
import argparse
from collections import deque

import flux
from flux import job

import utils


SUBMITTED_JOBS = deque()


def submit_cb(f):
    SUBMITTED_JOBS.append(job.submit_get_id(f))



def setup_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Submit a number of fixed-length jobs to Flux via Flux's Python bindings."
        )
    )
    parser.add_argument(
        "jobcount",
        type=int,
        help="Number of jobs to execute",
    )
    return parser


def main():
    implementation = "bulksubmit"
    start_time = time.perf_counter()
    args = setup_parser().parse_args()
    # open connection to broker
    h = flux.Flux()
    # create jobspec for sleep command
    compute_jobspec = job.JobspecV1.from_command(
        command=["true"],
        num_tasks=1,
        num_nodes=1,
        cores_per_task=1
    )
    compute_jobspec.cwd = os.getcwd()
    done = 0
    for _ in range(args.jobcount):
        job.submit_async(h, compute_jobspec, waitable=True).then(submit_cb)
    if h.reactor_run(h.get_reactor(), 0) < 0:
        h.fatal_error("reactor start failed")
    while done < args.jobcount:
        jobid, success, errstr = job.wait(h)
        if not success:
            print("wait: {} Error: {}".format(jobid, errstr))
        done += 1
    total_time = time.perf_counter() - start_time
    print("Total seconds: {}".format(total_time))
    utils.save_timing_data(args.jobcount, total_time, implementation)


if __name__ == '__main__':
    main()

# vim: tabstop=4 shiftwidth=4 expandtab
