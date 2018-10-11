#!/bin/bash

wget https://s3-us-west-2.amazonaws.com/cloud-data-server/mongo-hadoop-jar.zip

unzip mongo-hadoop-jar.zip

sudo mv mongo-hadoop-jar/* /usr/lib/hadoop-mapreduce/

rm -rf mongo-hadoop-*
