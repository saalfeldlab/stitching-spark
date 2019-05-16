import os
import sys
import subprocess

sys.dont_write_bytecode = True
curr_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(curr_script_dir))
from jar_path_util import get_jar_path
bin_path = get_jar_path()

flintstone_relpath = os.path.join('flintstone', 'flintstone-lsd.sh')
flintstone_path = os.path.join(curr_script_dir, flintstone_relpath)

def submit(java_classname, extra_args=[]):
    num_workers = int(sys.argv[1])
    args = extra_args + sys.argv[2:]
    submit_extended(num_workers, java_classname, args)

def submit_extended(num_workers, java_classname, args):
    subprocess.call([flintstone_path, str(num_workers), bin_path, java_classname] + args)