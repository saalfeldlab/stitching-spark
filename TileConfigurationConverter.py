import argparse
import json

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('path', type=str, help='path to initial tile configuration file')
	args = parser.parse_args()
	
	out_path = args.path + '.json'
	sep_ind = out_path.rindex('/') + 1
	abs_path_prefix = out_path[ : sep_ind ]
	try:
		out_file = open( out_path, 'w' )
	except IOError:
		# non-writable path, store in the current directory as a fallback
		out_path = out_path[sep_ind : ]
		out_file = open( out_path, 'w' )
	
	data = []
	with open(args.path) as in_file:
		for line in in_file.readlines():
			tokens = [t.strip() for t in line.strip().split()]
			
			#FIXME: temporary workaround for particular dataset
			wrong_suffix = '_decon.tif'
			if tokens[0].endswith(wrong_suffix):
				tokens[0] = tokens[0][ : len(tokens[0]) - len(wrong_suffix) ] + '.tif'

			data.append({ 'file': abs_path_prefix + tokens[0], 'position': [float(tokens[2]), float(tokens[1]), float(tokens[3])] })
	
	out_file.write(json.dumps(data))
	out_file.close()
