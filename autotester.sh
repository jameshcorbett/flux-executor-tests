#!/bin/bash
#SBATCH -N 1
#SBATCH -p pbatch
#SBATCH -t 120

set -e

base_jobcount=1000
iterations=3
flux="srun --mpi=none --mpibind=off --ntasks-per-node=1 /usr/workspace/corbett8/flux-core/src/cmd/flux start"

for (( i = 4; i < 6; i++ )); do
	jobcount=$((base_jobcount * (2 ** i)))
	echo "${jobcount}"
	for (( j = 0; j < iterations; j++ )); do
		$flux python3 bulksubmit.py $jobcount
		$flux python3 bulksubmit_executor.py -n $jobcount
		$flux python3 bulksubmit_executor.py -n $jobcount --events
	done
done
