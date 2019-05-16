#!/usr/bin/env python

# no parallelization is needed for this step
import sys
num_workers = 1
args = sys.argv[1:]

from submit import submit_extended
submit_extended(num_workers, 'org.janelia.stitching.ParseTilesImageList', args)