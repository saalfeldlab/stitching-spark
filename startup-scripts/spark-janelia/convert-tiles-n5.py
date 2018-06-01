#!/usr/bin/env python

import os
import sys
import subprocess

sys.dont_write_bytecode = True
curr_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(curr_script_dir))
from jar_path_util import get_jar_path
bin_path = get_jar_path()

flintstone_relpath = os.path.join('flintstone', 'flintstone.sh')
flintstone_path = os.path.join(curr_script_dir, flintstone_relpath)

os.environ['SPARK_VERSION'] = 'test'
os.environ['N_DRIVER_THREADS'] = '2'
os.environ['MEMORY_PER_NODE'] = '105'
os.environ['TERMINATE'] = '1'

nodes = int(sys.argv[1])

subprocess.call([flintstone_path, str(nodes), bin_path, 'org.janelia.stitching.TilesToN5ConverterSpark'] + sys.argv[2:])
