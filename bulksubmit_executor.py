#!/usr/bin/env python3

import time
import sys
import argparse
import concurrent.futures as cf

from flux.job import FluxExecutor, FluxEventExecutor, JobspecV1

import utils


def log(label, s):
    print(label + ": " + s)


def main():
    parser = argparse.ArgumentParser(
        description="Submit a command repeatedly using FluxExecutor"
    )
    parser.add_argument(
        "-n",
        "--njobs",
        type=int,
        metavar="N",
        help="Set the total number of jobs to run",
        default=100,
    )
    parser.add_argument(
        "--events",
        action="store_true",
        help="Use the EventExecutor",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command:
        args.command = ["true"]
    t0 = time.perf_counter()
    implementation = "bulksubmit_executor" if not args.events else "bulksubmit_event_executor"
    exec_type = FluxExecutor if not args.events else FluxEventExecutor
    with exec_type() as executor:
        compute_jobspec = JobspecV1.from_command(args.command)
        futures = [executor.submit(compute_jobspec) for _ in range(args.njobs)]
        # wait for jobs to complete
        done, not_done = cf.wait(futures, return_when=cf.FIRST_EXCEPTION)
        if not_done:
            raise ValueError()
    # print time summary
    total_time = time.perf_counter() - t0
    log(implementation, f"Ran {args.njobs} jobs in {total_time:.1f}s. {args.njobs / total_time:.1f} job/s")
    utils.save_timing_data(args.njobs, total_time, implementation)


if __name__ == "__main__":
    main()
