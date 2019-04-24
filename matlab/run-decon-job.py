#!/usr/bin/env python

import argparse
import json
import os
import subprocess
import copy
from time import sleep

run_decon_task_script_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run-decon-single-task.py')

def load_tile_metadata(args):
	channels_metadata = []
	for config_filepath in args.input_channels_paths:
		with open(config_filepath) as config_file:
			channels_metadata.append(json.load(config_file))
	return channels_metadata

def get_flatfield_paths(args):
	flatfield_paths = []
	for config_filepath in args.input_channels_paths:
		config_filename = os.path.basename(config_filepath)
		config_dirpath = os.path.dirname(os.path.abspath(config_filepath))
		config_base_name = config_filename[:config_filename.rfind('.')]
		flatfield_path_possibilities = [
			os.path.join(config_dirpath, config_base_name + '-flatfield'),
			os.path.join(config_dirpath, config_base_name + '-n5-flatfield')
		]
		found_flatfield_path = None
		for flatfield_path in flatfield_path_possibilities:
			if os.path.exists(flatfield_path):
				found_flatfield_path = flatfield_path
				break
		if found_flatfield_path is not None:
			print('found flatfields here: ' + found_flatfield_path)
			flatfield_paths.append(found_flatfield_path)
		else:
			print('flatfield dir does not exist, skip flatfield correction')
			flatfield_paths.append(None)
	return flatfield_paths

def get_background_intensities(args, flatfield_paths):
	if args.background_intensity is not None:
		print('use background value from the arguments: ' + str(args.background_intensity))
		return [args.background_intensity] * len(flatfield_paths)
	else:
		background_values = []
		for flatfield_path in flatfield_paths:
			if flatfield_path is not None:
				with open(os.path.join(flatfield_path, 'attributes.json')) as flatfield_metadata_file:
					flatfield_metadata = json.load(flatfield_metadata_file)
					flatfield_background_value = flatfield_metadata['pivotValue']
					print('got background value ' + str(flatfield_background_value) + ' from flatfield metadata: ' + flatfield_path)
					background_values.append(flatfield_background_value)
			else:
				print('no background value')
				background_values.append(None)
		return background_values

def submit_task(task, output_dirpath, index, cores_per_task):
	# save task to file
	task_filepath = os.path.join(output_dirpath, "task" + str(index))
	with open(task_filepath, 'w') as task_file:
		task_file.write(json.dumps(task))
	subprocess.Popen(['bsub', '-W', '01:00', '-o', '/dev/null', '-n', str(cores_per_task), run_decon_task_script_filepath, task_filepath])
	return task_filepath

def num_tasks_left(task_filepaths):
	# check how many task files are still there (completed jobs remove their task files)
	tasks_left = 0
	for task_filepath in task_filepaths:
		if os.path.exists(task_filepath):
			tasks_left += 1
	print('still ' + str(tasks_left) + ' tasks left')
	return tasks_left

def get_output_tile_filepaths(input_channels_metadata, output_dirpath):
	output_channels_tile_filepaths = []
	for channel in range(num_channels):
		output_tile_filepaths = {}
		for tile_metadata in input_channels_metadata[channel]:
			tile_filename = os.path.basename(tile_metadata['file'])
			decon_tile_filename = tile_filename[:tile_filename.rfind('.')] + '_decon' + tile_filename[tile_filename.rfind('.'):]
			decon_tile_output_filepath = os.path.join(output_dirpath, decon_tile_filename)
			output_tile_filepaths[tile_metadata['index']] = decon_tile_output_filepath
		output_channels_tile_filepaths.append(output_tile_filepaths)
	return output_channels_tile_filepaths

def save_decon_metadata(input_channels_metadata, channels_output_tile_filepaths, args):
	for channel, (tiles_metadata, config_filepath) in enumerate(zip(input_channels_metadata, args.input_channels_paths)):
		decon_tiles = []
		for tile in tiles_metadata:
			decon_tile = copy.deepcopy(tile)
			decon_tile['file'] = channels_output_tile_filepaths[channel][tile['index']]
			decon_tiles.append(decon_tile)
		config_decon_path = config_filepath[:config_filepath.rfind('.')] + '-decon' + config_filepath[config_filepath.rfind('.'):]
		with open(config_decon_path, 'w') as config_decon_file:
			config_decon_file.write(json.dumps(decon_tiles))

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--inputConfigurationPath', nargs='+', dest='input_channels_paths', help='Path to an input tile configuration file. Multiple configurations (channels) can be passed at once.')
	parser.add_argument('-p', '--psfPath', nargs='+', dest='psf_paths', help='Path to the point-spread function images. In case of multiple input channels, their corresponding PSFs must be passed in the same order.')
	parser.add_argument('-z', '--psfStepZ', type=float, dest='psf_z_step', help='PSF Z step in microns.')
	parser.add_argument('-n', '--numIterations', type=int, default=10, dest='num_iterations', help='Number of deconvolution iterations.')
	parser.add_argument('-v', '--backgroundValue', type=float, dest='background_intensity', default=None, help='Background intensity value which will be subtracted from the data and the PSF (one per input channel). If omitted, the pivot value estimated in the Flatfield Correction step will be used (default).')
	parser.add_argument('-c', '--coresPerTask', dest='cores_per_task', type=int, default=8, help='Number of CPU cores used by a single decon task.')
	args = parser.parse_args()

	num_channels = len(args.input_channels_paths)
	if num_channels != len(args.psf_paths):
		raise Exception('number of provided PSFs should be the same as number of input channels')

	# create output folders
	output_dirpath = os.path.join(os.path.dirname(os.path.abspath(args.input_channels_paths[0])), 'matlab_decon')
	if not os.path.exists(output_dirpath):
		os.makedirs(output_dirpath)

	channels_tile_metadata = load_tile_metadata(args)
	channels_flatfield_paths = get_flatfield_paths(args)
	channels_background_intensities = get_background_intensities(args, channels_flatfield_paths)
	channels_output_tile_filepaths = get_output_tile_filepaths(channels_tile_metadata, output_dirpath)

	# create list of all tasks
	tasks = []
	for channel in range(num_channels):
		for tile_metadata in channels_tile_metadata[channel]:
			tasks.append({
				'tile_filepath': tile_metadata['file'],
				'output_tile_filepath': channels_output_tile_filepaths[channel][tile_metadata['index']],
				'psf_filepath': args.psf_paths[channel],
				'flatfield_dirpath': channels_flatfield_paths[channel],
				'background_value': channels_background_intensities[channel],
				'data_z_resolution': tile_metadata['pixelResolution'][2],
				'psf_z_step': args.psf_z_step,
				'num_iterations': args.num_iterations
			})

	# submit tasks (skip tasks where target files already exist)
	task_filepaths = []
	for index, task in enumerate(tasks):
		if not os.path.exists(task['output_tile_filepath']):
			task_filepaths.append(submit_task(task, output_dirpath, index, args.cores_per_task))

	# wait until all tasks are completed
	while num_tasks_left(task_filepaths) > 0:
		sleep(30)

	# write output tile metadata
	channels_decon_metadata = save_decon_metadata(channels_tile_metadata, channels_output_tile_filepaths, args)

	print('Done')