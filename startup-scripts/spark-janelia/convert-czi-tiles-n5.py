#!/usr/bin/env python

# decrease number of parallel tasks on each worker node to let each task use more memory
import os
os.environ['N_EXECUTORS_PER_NODE'] = '3'
os.environ['N_CORES_PER_EXECUTOR'] = '3'

from submit import submit
submit('org.janelia.stitching.ConvertTilesToN5Spark')