#!/bin/bash

export SPARK_VERSION=2
export N_DRIVER_THREADS=2
export MEMORY_PER_NODE=115
export TERMINATE=1

N_NODES=$1;     shift

flintstone/flintstone.sh $N_NODES ../target/stitching-spark-0.0.1-SNAPSHOT.jar org.janelia.stitching.N5ToSliceTiffSpark $@