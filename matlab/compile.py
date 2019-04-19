#!/usr/bin/env python

import os
import subprocess

matlab_root_path = '/misc/local/matlab-2018b'
matlab_compiler = os.path.join(matlab_root_path, 'bin', 'mcc')
matlab_program_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'matlab_decon.m')

if __name__ == '__main__':
	subprocess.call([matlab_compiler, '-m', '-R', '-nojvm', '-v', matlab_program_filepath])