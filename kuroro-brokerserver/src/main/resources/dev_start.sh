#
# Copyright 2017 EpochArch.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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