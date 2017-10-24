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
#check env
ENV_FILE="/var/www/webapps/config/kuroro_env.sh"

source ${ENV_FILE}

export PATH=$JAVA_HOME/bin:$PATH

if [ ! -f "${ENV_FILE}" ];then
    echo "Env file not found, bye..."
    exit
fi

#backup gc log
if [ -f "${LOG_DIR}/kuroro-brokerserver-gc.log" ];then
    tail -n 3000 ${LOG_DIR}/kuroro-brokerserver-gc.log > ${LOG_DIR}/kuroro-brokerserver-gc.log.`date +-%Y-%m-%d-%H-%m-%S`.log
    echo "Gc log backup finished"
fi

STD_OUT="${LOG_DIR}/app_catalina.log"

if [ -f "$STD_OUT" ];then
    mv $STD_OUT $STD_OUT`date +-%Y-%m-%d-%H-%m-%S`
fi

echo

#shutdown
SHUTDOWN_BIN="${PRGDIR}/closeMonitor.py"
ps -ef | grep "kuroro.broker.MainBootStrap" |grep -v grep

echo

if [ $? -eq 0 ];then
    echo "Kuroro is alive,shutdown now"
    python ${SHUTDOWN_BIN}
else
    echo "Kuroro is not alive"
fi


#check kuroro pid
PID=$(ps -ef | grep "kuroro.broker.MainBootStrap" | grep -v grep | awk '{print $2}' | head -1)
if [ $PID > 0  ];then
  kill -9  $PID
  echo "Kill kuroro Pid  " + $PID
else
  echo "Kuroro is not alive, shutdown now"
fi



#end

JMX_PORT=3997

CP_OPTS="-cp ${PRGDIR}/config:${PRGDIR}/libs/*"

JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=${JMX_PORT} -Djava.rmi.server.hostname=${LISTEN_IP}"
GCLOG_OPTS="-Xloggc:${LOG_DIR}/kuroro-brokerserver-gc.log"
PRETEND="-Dtomcat=pretend"

OPTS="${CP_OPTS} ${JVM_OPTS} ${BROKER_OPTS} ${JMX_OPTS} ${GCLOG_OPTS} ${PRETEND}"

MAIN="com.epocharch.kuroro.broker.MainBootStrap"

echo "Starting in ${LISTEN_IP} ..."

echo ${LOG_DIR}/kuroro-brokerserver-boot.log

exec java ${OPTS} ${MAIN} $*> "$STD_OUT" 2>&1 &