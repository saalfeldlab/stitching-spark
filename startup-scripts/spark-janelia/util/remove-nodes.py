#!/usr/bin/env python

import os
import sys
import subprocess

curr_script_dir = os.path.dirname(os.path.realpath(__file__))
spark_janelia_file = os.path.join('flintstone', 'spark-janelia', 'spark-janelia-lsf')
spark_janelia_path = os.path.join(os.path.dirname(curr_script_dir), spark_janelia_file)

master_id = int(sys.argv[1])
nodes = int(sys.argv[2])

subprocess.call([spark_janelia_path, 'remove-workers', '-j', str(master_id), '-n', str(nodes), '-f'])