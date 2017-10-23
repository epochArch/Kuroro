#!/bin/bash
PRGDIR=`dirname "$0"`
LOCAL_IP=`ifconfig en0 | grep 'inet ' | awk '{ print $2}'`
if [ ! -d "/tmp/logs/kuroro" ] ; then
  mkdir -p "/tmp/logs/kuroro"
fi
JMX_PORT=3997
JAVA_OPTS="-cp ${PRGDIR}/config:${PRGDIR}/libs/* -server -Xms1g -Xmx1g -XX:PermSize=128m -XX:MaxPermSize=256m -XX:MaxDirectMemorySize=512m -XX:ParallelCMSThreads=8 -XX:+HeapDumpOnOutOfMemoryError  -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+ExplicitGCInvokesConcurrent -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly -XX:NewRatio=2 -Dglobal.block.queue.threshold=100 -Dglobal.block.queue.fetchsize=400 -Dglobal.producerServer.workerCount=0 -Dglobal.config.path=/Users/bill/Documents/work/env/test_jq -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
Broker_JAVA_OPTS="${JAVA_OPTS}  -Dcom.sun.management.jmxremote.port=${JMX_PORT} -Xloggc:/tmp/kuroro-brokerserver-gc.log"
Main_Class="com.epocharch.kuroro.broker.MainBootStrap"
STD_OUT="/tmp/kuroro-broker-std.out"
mv $STD_OUT $STD_OUT`date +-%Y%m%d%H`
Broker_JAVA_OPTS="${Broker_JAVA_OPTS} -Djava.rmi.server.hostname=$LOCAL_IP -DbrokerIp=$LOCAL_IP"
echo "starting as broker(brokerIp is $LOCAL_IP ) ..."
echo "output: $STD_OUT"
exec java $Broker_JAVA_OPTS $Main_Class $*> "$STD_OUT" &