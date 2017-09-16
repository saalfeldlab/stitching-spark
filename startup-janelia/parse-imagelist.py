import sys
import subprocess

subprocess.call(['java', '-cp', '../target/stitching-spark-0.0.1-SNAPSHOT.jar', 'org.janelia.stitching.ParseTilesImageList'] + sys.argv[1:])