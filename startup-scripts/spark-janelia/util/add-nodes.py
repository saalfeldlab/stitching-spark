#!/usr/bin/env python

import os
import sys
import subprocess

curr_script_dir = os.path.dirname(os.path.abspath(__file__))
spark_janelia_relpath = os.path.join('flintstone', 'spark-janelia', 'spark-janelia-lsf')
spark_janelia_path = os.path.join(os.path.dirname(curr_script_dir), spark_janelia_relpath)

spark_version = 'test'

master_id = int(sys.argv[1])
nodes = int(sys.argv[2])
runtime = sys.argv[3] if len(sys.argv) > 3 else None

subprocess.call([spark_janelia_path, 'add-workers', '-j', str(master_id), '-n', str(nodes), '-v', spark_version] + (['-t', runtime] if runtime is not None else None)
