import argparse
import json

def parse_coordinate(s):
	length = len(s)
	ind_from = 1 if s.startswith('(') else None
	ind_to = -1 if s.endswith(')') or s.endswith(',') else None
	return float(s[ind_from : ind_to])

def last_path_delimeter_index(s):
	delim = '\\' if '\\' in s else '/' 
	try:
		return s.rindex(delim)
	except ValueError:
		return None

def append_path_delimeter(s):
	if s.endswith('/') or s.endswith('\\'):
		return s
	delim = '\\' if '\\' in s else '/' 
	return s + delim

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('path', type=str, help='path to initial tile configuration file')
	parser.add_argument('images_folder', type=str, help='path to the folder where actual images are stored', nargs='?')
	args = parser.parse_args()
	
	out_path = (args.path[:-len('.txt')] if args.path.endswith('.txt') else args.path) + '.json'
	sep_ind = last_path_delimeter_index(out_path) + 1 if last_path_delimeter_index(out_path) is not None else None
	abs_path_prefix = append_path_delimeter(args.images_folder) if args.images_folder is not None else out_path[: sep_ind]
	try:
		out_file = open( out_path, 'w' )
	except IOError:
		# non-writable path, store in the current directory as a fallback
		out_path = out_path[sep_ind : ]
		out_file = open( out_path, 'w' )

	print 'Base images folder: ' + abs_path_prefix
	
	data = []
	index = 0
	dim = None
	with open(args.path) as in_file:
		for line in in_file.readlines():
			if line.startswith('#') or len(line.strip()) == 0:
				continue
			elif line.startswith('dim = '):
				dim = int(line.strip().split()[-1])
				print 'Detected dim = ' + str(dim)
				continue

			if dim is None:
				print 'Using default dim value = ' + str(dim)
				dim = 3

			tokens = [t.strip() for t in line.strip().split(';')]

			#FIXME: temporary workaround for particular dataset
			wrong_suffix = '_decon.tif'
			if tokens[0].endswith(wrong_suffix):
				tokens[0] = tokens[0][: len(tokens[0]) - len(wrong_suffix)] + '.tif'

			position_tokens = tokens[2].split()
			position = []
			for i in range(dim):
				position.append(parse_coordinate(position_tokens[i]))

			data.append({ 'index': index, 'file': abs_path_prefix + tokens[0], 'position': position })

			index += 1
	
	print 'Processed ' + str(index) + ' tiles'

	out_file.write(json.dumps(data))
	out_file.close()

	print 'Created output file: ' + out_path