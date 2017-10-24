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

#!/usr/bin/env bash
LOG_DIR=/Users/zhoufeiqiang/Applications/test/webapps

GLOBAL_CONFIG_PATH=/Users/zhoufeiqiang/Applications/test/config

ETH=eth0
LISTEN_IP=`ifconfig ${ETH} | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`

if [ -z "$LISTEN_IP" ]; then
    echo "IP addr not parsed, bye..."
    exit
fi

if [ ! -d "$LOG_DIR"  ] ; then
  mkdir -p ${LOG_DIR}
fi

JVM_OPTS="-server -Xms8g -Xmx8g -XX:PermSize=128m -XX:MaxPermSize=256m -XX:MaxDirectMemorySize=512m -XX:ParallelCMSThreads=8 -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+ExplicitGCInvokesConcurrent -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly -XX:NewRatio=2 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,address=9999,server=y,suspend=y -server"

BROKER_OPTS="-Dglobal.block.queue.threshold=20 -Dglobal.block.queue.fetchsize=300 -Dglobal.producerServer.workerCount=0 -Dglobal.config.path=${GLOBAL_CONFIG_PATH} -DbrokerIp=${LISTEN_IP}"

command -v python >/dev/null 2>&1 || { echo >&2 "Python required, bye..."; exit -1; }

echo "Kuroro env imported"