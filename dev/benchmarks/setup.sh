#!/bin/bash

# install Java
sudo yum install -y java-17-amazon-corretto-headless java-17-amazon-corretto-devel
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto


git clone https://github.com/apache/datafusion-benchmarks.git
