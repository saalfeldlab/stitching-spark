import os

def get_jar_path():
	curr_script_dir = os.path.dirname(os.path.abspath(__file__))
	base_folder = os.path.dirname(curr_script_dir)
	target_folder = os.path.join(base_folder, 'target');
	pom_properties_relpath = os.path.join('maven-archiver', 'pom.properties')
	pom_properties_path = os.path.join(target_folder, pom_properties_relpath)

	pom_properties = {}
	with open(pom_properties_path, 'r') as pom_properties_file:
		for pom_property in pom_properties_file.readlines():
			if pom_property[0] != '#':
				pom_property_split = pom_property.rstrip().split('=')
				pom_property_key, pom_property_value = pom_property_split[0], pom_property_split[1]
				pom_properties[pom_property_key] = pom_property_value

	if 'artifactId' not in pom_properties or 'version' not in pom_properties:
		raise Exception("artifactId/version fields are missing in pom.properties file")

	return os.path.join(target_folder, pom_properties['artifactId'] + '-' + pom_properties['version'] + '.jar')