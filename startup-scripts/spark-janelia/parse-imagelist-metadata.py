#!/usr/bin/env python

# no parallelization is needed for this step
import sys
num_workers = 1
args = sys.argv[1:]

# Avoid potential StackOverflowError when working with very large datasets (>10k tiles).
import os
os.environ['SPARK_OPTIONS'] = '--driver-java-options -Xss1g'

from submit import submit_extended
submit_extended(num_workers, 'org.janelia.stitching.ParseTilesImageList', args)