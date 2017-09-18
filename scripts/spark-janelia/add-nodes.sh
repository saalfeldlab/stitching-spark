#!/bin/bash

JOB_ID=$1;     shift
N_NODES=$1;     shift

SPARK_VERSION=2

spark-janelia/spark-janelia add-workers -j $JOB_ID -n $N_NODES -v $SPARK_VERSION -t $RUNTIME