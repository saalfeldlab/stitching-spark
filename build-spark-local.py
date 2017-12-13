#!/usr/bin/env python

import sys
sys.dont_write_bytecode = True

import os
from build import build

if __name__ == '__main__':
	base_folder = os.path.dirname(os.path.realpath(__file__))
	build_args = ['-P', 'spark-local']
	build(base_folder, build_args)
