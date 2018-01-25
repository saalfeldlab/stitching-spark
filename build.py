#!/usr/bin/env python

import os
import subprocess

def run_build(base_folder, build_args=[]):
	cmd_args = ['mvn', 'clean', 'package'] + build_args
	subprocess.call(cmd_args, cwd=base_folder)

if __name__ == '__main__':
	base_folder = os.path.dirname(os.path.abspath(__file__))
	run_build(base_folder)
