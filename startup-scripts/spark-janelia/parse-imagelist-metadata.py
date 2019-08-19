#!/usr/bin/env python

# no parallelization is needed for this step
import sys
num_workers = 1
args = sys.argv[1:]

# TODO: when working with very large datasets (>10k tiles), this sometimes may fail with StackOverflowError.
# The stack size needs to be increased, but there seems to be no way to do it via spark-submit.
# Consider using the local parser /startup-scripts/spark-local/parse-imagelist-metadata.py, or rework the parsing algorithm to be non-recursive.

from submit import submit_extended
submit_extended(num_workers, 'org.janelia.stitching.ParseTilesImageList', args)