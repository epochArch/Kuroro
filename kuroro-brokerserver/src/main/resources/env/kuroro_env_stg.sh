#!/bin/bash

LOG_DIR=/var/log/webapps/

ETH=eth0
LISTEN_IP=`ifconfig ${ETH} | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`

if [ -z "$LISTEN_IP" ]; then
    echo "IP addr not parsed, bye..."
    exit
fi

if [ ! -d "$LOG_DIR"  ] ; then
  mkdir -p "/var/log/webapps/"
fi

JVM_OPTS="-server -Xms1024m -Xmx1024m -XX:PermSize=128m -XX:MaxPermSize=256m -XX:MaxDirectMemorySize=512m -XX:ParallelCMSThreads=8 -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+ExplicitGCInvokesConcurrent -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly -XX:NewRatio=2 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"

BROKER_OPTS="-Dglobal.block.queue.threshold=30 -Dglobal.block.queue.fetchsize=300 -Dglobal.producerServer.workerCount=0 -Dglobal.config.path=/var/www/webapps/config -DbrokerIp=${LISTEN_IP}"

command -v python >/dev/null 2>&1 || { echo >&2 "Python required, bye..."; exit -1; }

echo "Kuroro env imported"