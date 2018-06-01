#!/usr/bin/env python

import os
import sys
import subprocess

from jar_path_util import get_local_jar_path, get_provided_jar_path
if os.path.exists(get_local_jar_path()):
	bin_path = get_local_jar_path()
else:
	bin_path = get_provided_jar_path()

subprocess.call(['java', '-cp', bin_path, 'org.janelia.stitching.TilesToN5ConverterMultithreaded'] + sys.argv[1:])