#!/usr/bin/env python

import os
import sys
import subprocess

curr_script_dir = os.path.dirname(os.path.realpath(__file__))
base_folder = os.path.dirname(os.path.dirname(curr_script_dir))
bin_file = os.path.join('target', 'stitching-spark-0.0.1-SNAPSHOT.jar')
bin_path = os.path.join(base_folder, bin_file)

subprocess.call(['java', '-cp', bin_path, 'org.janelia.stitching.TilesToN5ConverterMultithreaded'] + sys.argv[1:])