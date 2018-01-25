#!/usr/bin/env python

import os
import sys

sys.dont_write_bytecode = True
from build import run_build

if __name__ == '__main__':
	base_folder = os.path.dirname(os.path.abspath(__file__))
	build_args = ['-P', 'spark-local']
	run_build(base_folder, build_args)
