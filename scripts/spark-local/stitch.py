import sys
import subprocess

subprocess.call(['java', '-Dspark.master=local[*]' '-cp', '../../target/stitching-spark-0.0.1-SNAPSHOT.jar', 'org.janelia.stitching.StitchingSpark', '--stitch', '--mode', 'restitching-incremental'] + sys.argv[1:])