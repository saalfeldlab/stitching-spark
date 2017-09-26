#!/usr/bin/env python

import sys
import json
import os.path

if __name__ == '__main__':
	for config_filepath in sys.argv[1:]:
		if config_filepath == '-i':
			continue

		with open(config_filepath) as config_file:
			tiles = json.load(config_file)
			for tile in tiles:
				filepath = tile['file']
				folder = os.path.dirname(filepath)
				filename = os.path.basename(filepath)
				decon_folder = os.path.join(folder, 'matlab_decon')
				decon_filename = filename[:filename.rfind('.')] + '_decon' + filename[filename.rfind('.'):]
				decon_filepath = os.path.join(decon_folder, decon_filename)
				tile['file'] = decon_filepath

		config_decon_path = config_filepath[:config_filepath.rfind('.')] + '-decon' + config_filepath[config_filepath.rfind('.'):]
		print('saving to ' + config_decon_path)
		with open(config_decon_path, 'w') as config_decon_file:
			config_decon_file.write(json.dumps(tiles))