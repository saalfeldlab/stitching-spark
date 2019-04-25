#!/usr/bin/env python

import os
import sys
import subprocess

logfile_dirpath = os.path.join(os.path.expanduser('~'), '.matlab_decon')
run_decon_job_script_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run-decon-job.py')

if __name__ == '__main__':
	if not os.path.exists(logfile_dirpath):
		os.makedirs(logfile_dirpath)
	logfile_path = os.path.join(logfile_dirpath, 'driver-%J.out')

	subprocess.call([
		'bsub',
		'-W', '12:00',
		'-o', logfile_path,
		'-n', '1',
		run_decon_job_script_filepath] + sys.argv[1:]
	)