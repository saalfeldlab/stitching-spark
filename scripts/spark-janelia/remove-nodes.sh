#!/bin/bash

JOB_ID=$1;     shift
N_NODES=$1;     shift

spark-janelia/spark-janelia remove-workers -j $JOB_ID -n $N_NODES -f