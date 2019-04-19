#!/usr/bin/env python

import json
import os
import sys
import subprocess

from compile import matlab_root_path # use the same runtime that was used for compilation

matlab_program_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'matlab_decon')
required_library_paths = [
	'bin/glnxa64',
	'runtime/glnxa64',
	'sys/os/glnxa64',
	'sys/java/jre/glnxa64/jre/lib/amd64/native_threads',
	'sys/java/jre/glnxa64/jre/lib/amd64/server',
	'sys/java/jre/glnxa64/jre/lib/amd64'
]

def set_env_vars():
	full_required_library_paths = ':'.join([os.path.join(matlab_root_path, library_path) for library_path in required_library_paths])
	curr_library_path_var = os.environ.get('LD_LIBRARY_PATH') or ''
	os.environ['LD_LIBRARY_PATH'] = curr_library_path_var + ':' + full_required_library_paths
	os.environ['XAPPLRESDIR'] = os.path.join(matlab_root_path, 'X11/app-defaults')
	os.environ['MCR_INHIBIT_CTF_LOCK'] = '1'

if __name__ == '__main__':
	task_filepath = sys.argv[1]

	# load task from file
	task = None
	with open(task_filepath, 'r') as task_file:
		task = json.load(task_file)

	flatfields_dirpath_or_empty = '' if task['flatfield_dirpath'] is None else task['flatfield_dirpath']

	# call matlab
	set_env_vars()
	subprocess.check_call([matlab_program_filepath,
		task['tile_filepath'],
		task['output_tile_filepath'],
		task['psf_filepath'],
		flatfields_dirpath_or_empty,
		str(task['background_value']),
		str(task['data_z_resolution']),
		str(task['psf_z_step']),
		str(task['num_iterations'])
	])

	# completed successfully, delete the task file to mark it as completed
	os.rename(task_filepath, task_filepath + '_completed')