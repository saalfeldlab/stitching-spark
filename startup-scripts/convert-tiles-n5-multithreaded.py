#!/usr/bin/env python

import os
import sys
import subprocess

from jar_path_util import get_jar_path
bin_path = get_jar_path()

subprocess.call(['java', '-cp', bin_path, 'org.janelia.stitching.TilesToN5ConverterMultithreaded'] + sys.argv[1:])